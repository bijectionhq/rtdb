use rtdb_types::StructFields;
use std::collections::{BTreeMap, VecDeque};
use std::sync::Arc;

use async_trait::async_trait;
use prost::Message;
use arrow_array::Array;
use rtdb_columnar::{MemoryBudget, MemoryReservation};
use rtdb_format::{envelope::ReplicaBody, data::{StoredEvent, StoredOperation}, generated::xtdb::log::proto::{ResolvedTx as WireResolvedTx, resolved_tx::DbOp}};
use rtdb_format::transit::{self, TransitValue as T};
use rtdb_index::{Snapshot, StagedWrite, TransactionBuffer};
use rtdb_query::{CatalogSnapshot, DatabaseSnapshot, PreparedQuery, QueryContext, QueryCursor, QueryOptions};
use rtdb_storage::cache::BufferPool;
use rtdb_types::{Anomaly, AnomalyCategory, Iid, Interval, LogicalType, Result, ScalarType, SourcePosition, TableRef, TimeUnit, Timestamp, TransactionKey, TxId, Value};
use tokio_util::sync::CancellationToken;

use crate::{SourceTransaction, SubmitOptions, TransactionResult, TxOp, UserMetadata, source::{Operation, decode_arguments, is_reserved_write_schema, table_name}};

pub trait TransactionAuthorization: Send + Sync {
    /// Rejects an absent principal or a principal without the reference superuser authority.
    fn check_role_membership(&self, database: &str, principal: Option<&str>) -> Result<()>;
}

pub struct ResolveContext {
    pub database: String,
    pub storage: Arc<BufferPool>,
    pub memory: Arc<MemoryBudget>,
    pub query_options: QueryOptions,
    pub scan_accelerators: Arc<[Arc<dyn rtdb_query::ScanAccelerator>]>,
    pub cancellation: CancellationToken,
    pub default_tz: String,
    pub authorization: Arc<dyn TransactionAuthorization>,
    pub execution_observer: Option<Arc<dyn rtdb_plan::ExecutionObserver>>,
    pub tracer: Option<Arc<dyn rtdb_plan::Tracer>>,
}

impl ResolveContext {
    fn query_context(&self, snapshot: Snapshot, time: Timestamp, timezone: &str, parent: Option<rtdb_plan::SpanContext>) -> Result<Arc<QueryContext>> {
        let catalog = CatalogSnapshot::new(self.database.clone(), BTreeMap::from([(self.database.clone(), DatabaseSnapshot { partitions: vec![snapshot], storage: Arc::clone(&self.storage) })]), true)?;
        Ok(QueryContext::new(catalog, Arc::clone(&self.memory), time, timezone, self.query_options.clone(), &self.cancellation)?
            .with_accelerators(Arc::clone(&self.scan_accelerators)).with_tracer(self.tracer.clone(), parent))
    }
    fn transaction_span(&self, parent: Option<rtdb_plan::SpanContext>, name: &str, attributes: &[(&str, rtdb_plan::TraceValue<'_>)]) -> Option<Box<dyn rtdb_plan::TraceSpan>> {
        self.tracer.as_ref().filter(|tracer| tracer.transaction_enabled()).and_then(|tracer| tracer.start(parent, name, attributes))
    }
    fn check_cancelled(&self) -> Result<()> {
        if self.cancellation.is_cancelled() { Err(Anomaly::new(AnomalyCategory::Interrupted, "xtdb/transaction-cancelled", "Transaction resolution was cancelled")) } else { Ok(()) }
    }
}

pub struct OwnedReplicaBody { body: ReplicaBody, _reservation: Arc<MemoryReservation> }
impl OwnedReplicaBody { pub fn body(&self) -> &ReplicaBody { &self.body } }

pub struct ResolvedTx {
    writes: Arc<StagedWrite>,
    result: TransactionResult,
    source_position: SourcePosition,
    external_token: Option<Vec<u8>>,
    db_op: Option<DbOp>,
    _reservation: Arc<MemoryReservation>,
}
impl ResolvedTx {
    pub fn writes(&self) -> &Arc<StagedWrite> { &self.writes }
    pub fn result(&self) -> &TransactionResult { &self.result }
    pub fn source_position(&self) -> SourcePosition { self.source_position }
    pub fn external_token(&self) -> Option<&[u8]> { self.external_token.as_deref() }
    pub fn db_op(&self) -> Option<&DbOp> { self.db_op.as_ref() }

    /// Serializes only this transaction's deltas, including zero-row schema declarations.
    /// The append owner supplies the term and sequence after obtaining this body.
    pub fn to_replica_body(&self, memory: &Arc<MemoryBudget>) -> Result<OwnedReplicaBody> {
        let mut tables = Vec::new();
        let mut bound = 65_536usize.checked_add(db_op_bytes(self.db_op.as_ref())?).ok_or_else(overflow)?;
        for (table, write) in self.writes.tables() {
            let batch = write.batch().ok_or_else(|| Anomaly::fault("rtdb.tx/missing-table-batch", "Resolved table declaration has no Arrow batch"))?;
            let bytes = crate::ipc::encode(batch.arrow().schema().as_ref(), [batch.arrow()], memory)?;
            let name = format!("{}/{}", table.schema, table.table);
            bound = bound.checked_add(bytes.as_slice().len()).and_then(|bound| bound.checked_add(name.capacity())).and_then(|bound| bound.checked_add(512)).ok_or_else(overflow)?;
            tables.push((name, bytes));
        }
        let error = self.result.encoded_error()?;
        bound = bound.checked_add(error.as_ref().map_or(0, |error| error.as_slice().len())).and_then(|bound| bound.checked_add(self.external_token.as_ref().map_or(0, Vec::capacity))).ok_or_else(overflow)?;
        let reservation = memory.reserve(bound)?;
        let key = self.result.tx_key();
        let body = ReplicaBody::ResolvedTx(WireResolvedTx {
            tx_id: Some(key.tx_id.0), system_time_micros: Some(key.system_time.0), committed: Some(self.result.committed()), error: error.map(|error| error.as_slice().to_vec()),
            table_data: tables.into_iter().map(|(name, bytes)| (name, bytes.as_slice().to_vec())).collect(),
            db_op: self.db_op.clone(), external_source_token: self.external_token.clone(), src_msg_id: Some(self.source_position.0),
        });
        Ok(OwnedReplicaBody { body, _reservation: reservation })
    }
}

pub struct TxResolver {
    resolved_head: Option<TransactionKey>,
    source_position: SourcePosition,
    external_token: Option<Vec<u8>>,
    queue: VecDeque<Arc<ResolvedTx>>,
    memory: Arc<MemoryBudget>,
    token_reservation: Arc<MemoryReservation>,
    queue_reservation: Arc<MemoryReservation>,
}
impl TxResolver {
    pub fn new(seed: Option<TransactionKey>, source_position: SourcePosition, external_token: Option<Vec<u8>>, memory: &Arc<MemoryBudget>) -> Result<Self> {
        let token_reservation = memory.reserve(external_token.as_ref().map_or(0, Vec::capacity).checked_add(512).ok_or_else(overflow)?)?;
        let queue_reservation = memory.reserve(128)?;
        Ok(Self { resolved_head: seed, source_position, external_token, queue: VecDeque::new(), memory: Arc::clone(memory), token_reservation, queue_reservation })
    }
    pub fn resolved_head(&self) -> Option<TransactionKey> { self.resolved_head }
    pub fn source_position(&self) -> SourcePosition { self.source_position }
    pub fn external_token(&self) -> Option<&[u8]> { self.external_token.as_deref() }
    pub fn staged_rows(&self) -> Result<u64> { self.queue.iter().try_fold(0u64, |rows, tx| rows.checked_add(tx.writes.row_count()?).ok_or_else(overflow)) }
    pub fn advance_source(&mut self, position: SourcePosition) -> Result<()> {
        if position < self.source_position { return Err(Anomaly::fault("rtdb.tx/source-watermark", "Resolve-side source watermark moved backwards")); }
        self.source_position = position;
        Ok(())
    }
    /// Transfers the oldest staged value before the read-back caller validates and applies it.
    pub fn take_head(&mut self) -> Option<Arc<ResolvedTx>> { self.queue.pop_front() }
    pub fn discard_staged(&mut self) { self.queue.clear(); }
    pub fn staged_db_op(&self, name: &str) -> Option<&DbOp> {
        self.queue.iter().rev().filter_map(|tx| tx.db_op()).find(|operation| match operation {
            DbOp::AttachDatabase(attach) => attach.db_name.as_deref() == Some(name),
            DbOp::DetachDatabase(detach) => detach.db_name.as_deref() == Some(name),
        })
    }
    pub fn resolving_snapshot(&self, durable: Snapshot) -> Result<Snapshot> {
        let writes = self.queue.iter().map(|tx| Arc::clone(&tx.writes)).collect::<Vec<_>>();
        let snapshot = durable.with_staged(&writes)?;
        if snapshot.tx_basis() != self.resolved_head { return Err(Anomaly::fault("rtdb.tx/resolved-basis", "Resolver head differs from durable and staged snapshots")); }
        Ok(snapshot)
    }

    /// Stages the skipped transaction outcome after its source payload has been persisted for inspection.
    pub fn resolve_skipped(&mut self, durable: Snapshot, position: SourcePosition, log_time: Timestamp, context: &ResolveContext) -> Result<Arc<ResolvedTx>> {
        let _span = context.transaction_span(None, "xtdb.transaction", &[("operations.count", rtdb_plan::TraceValue::Unsigned(0))]);
        context.check_cancelled()?;
        let base = self.resolving_snapshot(durable)?;
        let key = TransactionKey { tx_id: TxId(position.0), system_time: self.smooth(log_time)? };
        let options = SubmitOptions { default_tz: context.default_tz.clone(), ..SubmitOptions::default() };
        let mut tx = OpenTransaction::new(base, key, &options, context)?;
        let error = crate::RetainedAnomaly::new(Anomaly::fault("xtdb/skipped-tx", "Transaction was skipped"), &context.memory)?;
        let result = TransactionResult::Aborted { tx_key: key, error: Some(error) };
        tx.write_result(&result, None)?;
        self.stage(tx.buffer.stage()?, result, position, None, None, context)
    }

    pub async fn resolve_source(&mut self, durable: Snapshot, position: SourcePosition, log_time: Timestamp, request: SourceTransaction, context: &ResolveContext) -> Result<Arc<ResolvedTx>> {
        let _sample = rtdb_plan::ExecutionSample::new(context.execution_observer.clone(), rtdb_plan::ExecutionTimer::TxOp);
        let span = context.transaction_span(None, "xtdb.transaction", &[("operations.count", rtdb_plan::TraceValue::Unsigned(request.operations.len() as u64))]);
        context.check_cancelled()?;
        let base = self.resolving_snapshot(durable)?;
        let default_time = self.smooth(log_time)?;
        let invalid_time = request.options.system_time.zip(self.resolved_head)
            .filter(|(time, previous)| !crate::system_time::advances(Some(previous.system_time.0), time.0));
        let behind_log = request.options.system_time.filter(|time| {
            *time < log_time && request.options.user_metadata.as_ref().is_some_and(UserMetadata::requires_ordered_system_time)
        });
        let key = TransactionKey {
            tx_id: TxId(position.0),
            system_time: if invalid_time.is_some() || behind_log.is_some() {
                default_time
            } else {
                request.options.system_time.unwrap_or(default_time)
            },
        };
        let mut tx = OpenTransaction::new(base.clone(), key, &request.options, context)?;
        tx.trace_parent = span.as_ref().map(|span| span.context());
        let outcome = if let Some((requested, previous)) = invalid_time {
            Err(Anomaly::incorrect("invalid-system-time", "specified system-time must be later than current tx")
                .with_context("tx-key", tx_key_context(TransactionKey { tx_id: TxId(position.0), system_time: requested })?)
                .with_context("latest-completed-tx", tx_key_context(previous)?))
        } else if let Some(requested) = behind_log {
            Err(Anomaly::incorrect("rtdb/ordered-system-time", "specified system-time precedes the source-log timestamp")
                .with_context("tx-key", tx_key_context(TransactionKey { tx_id: TxId(position.0), system_time: requested })?)
                .with_context("log-time", transit::to_json_value(&T::instant(log_time.into())?)?))
        } else {
            let mut outcome = Ok(());
            for (index, operation) in request.operations.iter().enumerate() {
                context.check_cancelled()?;
                if let Err(error) = validate_document_operation(operation) {
                    outcome = Err(error);
                    break;
                }
                if let Err(error) = tx.apply(operation).await {
                    let error = match document_table(operation) {
                        Some(table) if !error.context.contains_key("table") => error.with_transit_context("table", &T::table_ref(table))?,
                        _ => error,
                    };
                    outcome = Err(error.with_context("tx-op-idx", index.into()).with_context("tx-key", tx_key_context(key)?));
                    break;
                }
            }
            outcome
        };
        let result = match outcome {
            Ok(()) => TransactionResult::Committed { tx_key: key },
            Err(error) if error.is_caller_error() => {
                if let Some(observer) = &context.execution_observer { observer.record_error(rtdb_plan::ExecutionError::Transaction); }
                drop(tx);
                tx = OpenTransaction::new(base, key, &request.options, context)?;
                TransactionResult::Aborted { tx_key: key, error: Some(crate::RetainedAnomaly::new(error, &context.memory)?) }
            }
            Err(error) => return Err(error),
        };
        tx.write_result(&result, request.options.user_metadata.as_ref())?;
        context.check_cancelled()?;
        self.stage(tx.buffer.stage()?, result, position, None, None, context)
    }

    pub async fn resolve_external(&mut self, durable: Snapshot, now: Timestamp, system_time: Option<Timestamp>, token: Option<Vec<u8>>, writer: &mut dyn TransactionWriter, context: &ResolveContext) -> Result<Arc<ResolvedTx>> {
        context.check_cancelled()?;
        let base = self.resolving_snapshot(durable)?;
        let id = self.resolved_head.map_or(-1, |key| key.tx_id.0).checked_add(1).ok_or_else(overflow)?;
        let key = TransactionKey { tx_id: TxId(id), system_time: self.smooth(system_time.unwrap_or(now))? };
        let options = SubmitOptions { default_tz: context.default_tz.clone(), ..SubmitOptions::default() };
        let mut tx = OpenTransaction::new(base.clone(), key, &options, context)?;
        let (result, metadata) = match writer.write(&mut tx).await? {
            WriterResult::Committed { user_metadata } => (TransactionResult::Committed { tx_key: key }, user_metadata),
            WriterResult::Aborted { error, user_metadata } => {
                if let Some(observer) = &context.execution_observer { observer.record_error(rtdb_plan::ExecutionError::Transaction); }
                drop(tx); tx = OpenTransaction::new(base, key, &options, context)?;
                (TransactionResult::Aborted { tx_key: key, error: Some(crate::RetainedAnomaly::new(error, &context.memory)?) }, user_metadata)
            }
        };
        tx.write_result(&result, metadata.as_ref())?;
        context.check_cancelled()?;
        self.stage(tx.buffer.stage()?, result, self.source_position, token, None, context)
    }

    pub fn resolve_db_op(&mut self, durable: Snapshot, position: SourcePosition, log_time: Timestamp, operation: DbOp, verdict: Result<()>, context: &ResolveContext) -> Result<Arc<ResolvedTx>> {
        let base = self.resolving_snapshot(durable)?;
        let key = TransactionKey { tx_id: TxId(position.0), system_time: self.smooth(log_time)? };
        let (result, operation) = match verdict {
            Ok(()) => (TransactionResult::Committed { tx_key: key }, Some(operation)),
            Err(error) if error.is_caller_error() => (TransactionResult::Aborted { tx_key: key, error: Some(crate::RetainedAnomaly::new(error, &context.memory)?) }, None),
            Err(error) => return Err(error),
        };
        let options = SubmitOptions { default_tz: context.default_tz.clone(), ..SubmitOptions::default() };
        let mut tx = OpenTransaction::new(base, key, &options, context)?;
        tx.write_result(&result, None)?;
        context.check_cancelled()?;
        self.stage(tx.buffer.stage()?, result, position, None, operation, context)
    }

    fn smooth(&self, candidate: Timestamp) -> Result<Timestamp> {
        crate::system_time::automatic_time(self.previous_system_time(), candidate.0)
            .map(Timestamp)
            .ok_or_else(|| Anomaly::incorrect("xtdb/time-overflow", "Timestamp has no following chronon"))
    }

    fn previous_system_time(&self) -> Option<i64> { self.resolved_head.map(|key| key.system_time.0) }

    fn stage(&mut self, writes: StagedWrite, result: TransactionResult, position: SourcePosition, token: Option<Vec<u8>>, db_op: Option<DbOp>, context: &ResolveContext) -> Result<Arc<ResolvedTx>> {
        if position < self.source_position { return Err(Anomaly::fault("rtdb.tx/source-watermark", "Resolve-side source watermark moved backwards")); }
        let key = result.tx_key();
        if writes.tx_key() != key || !crate::system_time::advances(self.previous_system_time(), key.system_time.0) {
            return Err(Anomaly::fault("rtdb.tx/system-time", "Resolved transaction does not advance its predecessor's system time"));
        }
        let bound = 4096usize.checked_add(token.as_ref().map_or(0, Vec::capacity)).and_then(|bytes| bytes.checked_add(db_op_bytes(db_op.as_ref()).ok()?)).and_then(|bytes| bytes.checked_add(result.error().map(crate::values::anomaly_bytes).transpose().ok()?.unwrap_or(0))).ok_or_else(overflow)?;
        let reservation = context.memory.reserve(bound)?;
        let next_token_charge = token.as_ref().map(|token| self.memory.reserve(token.capacity().checked_add(512).ok_or_else(overflow)?)).transpose()?;
        if self.queue.len() == self.queue.capacity() {
            let capacity = self.queue.capacity().max(4).checked_mul(2).ok_or_else(overflow)?;
            let charge = self.memory.reserve(capacity.checked_mul(std::mem::size_of::<Arc<ResolvedTx>>()).and_then(|bytes| bytes.checked_add(128)).ok_or_else(overflow)?)?;
            self.queue.reserve_exact(capacity - self.queue.len());
            self.queue_reservation = charge;
        }
        let resolved = Arc::new(ResolvedTx { writes: Arc::new(writes), result, source_position: position, external_token: token, db_op, _reservation: reservation });
        self.queue.push_back(Arc::clone(&resolved));
        self.resolved_head = Some(key);
        self.source_position = position;
        if let Some(token) = &resolved.external_token {
            self.external_token = Some(token.clone());
            self.token_reservation = next_token_charge.ok_or_else(|| Anomaly::fault("rtdb.tx/token-reservation", "External token has no reservation"))?;
        }
        Ok(resolved)
    }
}

#[async_trait]
pub trait TransactionWriter: Send {
    async fn write(&mut self, transaction: &mut OpenTransaction<'_>) -> Result<WriterResult>;
}
pub enum WriterResult { Committed { user_metadata: Option<UserMetadata> }, Aborted { error: Anomaly, user_metadata: Option<UserMetadata> } }

pub struct OpenTransaction<'a> { buffer: TransactionBuffer, options: &'a SubmitOptions, context: &'a ResolveContext, trace_parent: Option<rtdb_plan::SpanContext> }
impl<'a> OpenTransaction<'a> {
    fn new(base: Snapshot, key: TransactionKey, options: &'a SubmitOptions, context: &'a ResolveContext) -> Result<Self> {
        Ok(Self { buffer: TransactionBuffer::new(base, key, Arc::clone(&context.memory))?, options, context, trace_parent: None })
    }
    pub fn tx_key(&self) -> TransactionKey { self.buffer.tx_key() }
    pub fn snapshot(&self) -> Result<Snapshot> { self.buffer.snapshot() }
    pub fn append(&mut self, table: TableRef, event: StoredEvent) -> Result<()> { self.context.check_cancelled()?; self.buffer.append(table, event) }
    pub fn declare_table(&mut self, table: TableRef, columns: impl IntoIterator<Item = String>) -> Result<()> { check_table(&table)?; self.buffer.declare_table(table, columns) }

    pub async fn open_query(&self, sql: &str, arguments: Vec<Value>) -> Result<QueryCursor> {
        let context = self.context.query_context(self.snapshot()?, self.tx_key().system_time, &self.options.default_tz, self.trace_parent)?;
        let options = rtdb_sql::PrepareOptions { parameter_types: arguments.iter().enumerate().map(|(index, value)| (index, rtdb_columnar::execution_type(&value.logical_type()))).collect(), default_timezone: context.timezone.clone(), generate_series_end_exclusive: context.options.generate_series_end_exclusive, ..Default::default() };
        let query = {
            let _syntax = admit_sql_syntax(sql, &context.memory)?;
            let prepared = rtdb_sql::prepare_in(sql, &context.catalog, &options, &context.memory)?;
            PreparedQuery::from_prepared(prepared, &context.memory)?
        };
        query.execute_with_text(context, arguments, Some(sql)).await
    }

    pub async fn execute_sql(&mut self, sql: &str, arguments: Option<&[Value]>) -> Result<()> {
        self.execute_sql_typed(sql, arguments, None, &mut None).await
    }

    /// `planned` carries one operation's mutation from each argument row to the next, which reuses it while the
    /// transaction's catalog answers the planning's lookups the same.
    async fn execute_sql_typed(&mut self, sql: &str, arguments: Option<&[Value]>, types: Option<&[rtdb_types::LogicalType]>, planned: &mut Option<PlannedMutation>) -> Result<()> {
        self.context.check_cancelled()?;
        // A statement whose plan reads no table can't see the transaction's own writes, so it skips extending the snapshot with them.
        let snapshot = if planned.as_ref().is_some_and(|planned| !planned.reads) { self.buffer.base_snapshot() } else { self.snapshot()? };
        let context = self.context.query_context(snapshot, self.tx_key().system_time, &self.options.default_tz, self.trace_parent)?;
        let parameter_types = match types {
            Some(types) => types.iter().map(rtdb_columnar::execution_type).enumerate().collect(),
            None => arguments.unwrap_or_default().iter().enumerate().map(|(index, value)| (index, rtdb_columnar::execution_type(&value.logical_type()))).collect(),
        };
        let options = rtdb_sql::PrepareOptions { parameter_types, default_timezone: context.timezone.clone(), generate_series_end_exclusive: context.options.generate_series_end_exclusive, ..Default::default() };
        let with_sql = |error: Anomaly| error.with_context("sql", sql.into());
        let syntax = admit_sql_syntax(sql, &context.memory).map_err(with_sql)?;
        let reused = match planned.as_ref().filter(|planned| planned.options == options) {
            Some(planned) if planned.lookups.replay(&context.catalog).map_err(with_sql)? => Some(planned.mutation.clone()),
            _ => None,
        };
        let mutation = match reused {
            Some(mutation) => mutation,
            None => {
                let recording = rtdb_plan::RecordingCatalog::new(&context.catalog);
                let mutation = rtdb_sql::prepare_tx_in(sql, &recording, &options, &context.memory).map_err(with_sql)?;
                *planned = recording.finish().map(|lookups| PlannedMutation { options, lookups, reads: reads_tables(&mutation), mutation: mutation.clone() });
                mutation
            }
        };
        self.execute_mutation(mutation, context, arguments, Some(syntax)).await.map_err(with_sql)
    }

    async fn ordered_index_exists(&self, name: &str) -> Result<bool> {
        if !self.snapshot()?.contains_table(&TableRef::new("xt", "ordered_indexes")) { return Ok(false); }
        let mut query = self.open_query("SELECT _id FROM xt.ordered_indexes WHERE _id = ?", vec![Value::Utf8(name.into())]).await?;
        Ok(query.next_batch().await?.is_some())
    }

    async fn execute_mutation(&mut self, mutation: rtdb_sql::Mutation, context: Arc<QueryContext>, arguments: Option<&[Value]>, syntax: Option<Arc<MemoryReservation>>) -> Result<()> {
        use rtdb_sql::Mutation;
        let (table, input, kind) = match mutation {
            Mutation::Put { table, input } => (Some(table), input, MutationKind::Put),
            Mutation::Delete { table, input } => (Some(table), input, MutationKind::Delete),
            Mutation::Erase { table, input } => (Some(table), input, MutationKind::Erase),
            Mutation::Assert { input, message } => {
                let mut cursor = execute(input, context, arguments, syntax).await?;
                if cursor.next_batch().await?.is_none() { return Err(Anomaly::new(AnomalyCategory::Conflict, "xtdb/assert-failed", message.unwrap_or_else(|| "Assert failed".into()))); }
                return Ok(());
            }
            Mutation::CreateTable { table, columns } => return self.declare_table(table.table, columns),
            Mutation::CreateSearchIndex { name, table, columns, history } => {
                if table.database != self.context.database { return Err(Anomaly::incorrect("xtdb/table-not-found", "A search index cannot cover another database's table")); }
                check_search_index_name(&name)?;
                let document = rtdb_types::StructFields::from([
                    ("_id".into(), Value::Utf8(name)), ("schema_name".into(), Value::Utf8(table.table.schema.clone())), ("table_name".into(), Value::Utf8(table.table.table.clone())),
                    ("columns".into(), Value::list(columns.into_iter().map(Value::Utf8).collect())), ("analyzer".into(), Value::Utf8(rtdb_text::SIMPLE_ANALYZER.into())),
                    ("version".into(), Value::I64(self.tx_key().tx_id.0)), ("history".into(), Value::Bool(history)),
                ]);
                return self.buffer.put(TableRef::new("xt", "search_indexes"), document, interval(self.tx_key().system_time, Timestamp::MAX)?);
            }
            Mutation::DropSearchIndex { name } => {
                check_search_index_name(&name)?;
                return self.buffer.delete(TableRef::new("xt", "search_indexes"), &rtdb_types::EntityId::String(name), interval(self.tx_key().system_time, Timestamp::MAX)?);
            }
            Mutation::CreateOrderedIndex { name, table, columns, if_not_exists } => {
                if table.database != self.context.database { return Err(Anomaly::incorrect("xtdb/table-not-found", "An ordered index must cover a table in this database")); }
                check_table(&table.table)?;
                check_ordered_index_name(&name)?;
                if columns.iter().collect::<std::collections::BTreeSet<_>>().len() != columns.len()
                    || columns.iter().any(|column| matches!(column.as_str(), "_iid" | "_valid_from" | "_valid_to" | "_valid_time" | "_system_from" | "_system_to" | "_system_time" | "_recency"))
                {
                    return Err(Anomaly::incorrect("xtdb/invalid-index-columns", "An ordered index requires distinct stored columns"));
                }
                if self.ordered_index_exists(&name).await? {
                    if if_not_exists { return Ok(()); }
                    return Err(Anomaly::incorrect("xtdb/index-already-exists", format!("Index {name} already exists")));
                }
                let document = StructFields::from([
                    ("_id".into(), Value::Utf8(name)),
                    ("schema_name".into(), Value::Utf8(table.table.schema)),
                    ("table_name".into(), Value::Utf8(table.table.table)),
                    ("columns".into(), Value::list(columns.into_iter().map(Value::Utf8).collect())),
                    ("version".into(), Value::I64(self.tx_key().tx_id.0)),
                ]);
                return self.buffer.put(TableRef::new("xt", "ordered_indexes"), document, interval(self.tx_key().system_time, Timestamp::MAX)?);
            }
            Mutation::DropOrderedIndex { name } => {
                check_ordered_index_name(&name)?;
                if !self.ordered_index_exists(&name).await? {
                    return Err(Anomaly::incorrect("xtdb/index-not-found", format!("Index {name} does not exist")));
                }
                return self.buffer.delete(TableRef::new("xt", "ordered_indexes"), &rtdb_types::EntityId::String(name), interval(self.tx_key().system_time, Timestamp::MAX)?);
            }
            Mutation::GrantRole { user, role, revoke } => {
                if self.context.database != "xtdb" { return Err(Anomaly::incorrect("xtdb/role-membership-non-primary", "Role membership can only be managed on the primary 'xtdb' database.").with_context("db", self.context.database.clone().into())); }
                self.context.authorization.check_role_membership(&self.context.database, self.options.user.as_deref())?;
                use sha2::{Digest, Sha256};
                let length = i32::try_from(user.len()).map_err(|_| overflow())?;
                let mut hash = Sha256::new();
                hash.update(length.to_be_bytes());
                hash.update(user.as_bytes());
                hash.update(role.as_bytes());
                let digest = hash.finalize();
                let iid = Iid::from_slice(&digest[..16])?;
                let operation = if revoke { StoredOperation::Delete } else { StoredOperation::Put(StructFields::from([("user".into(), Value::Utf8(user)), ("role".into(), Value::Utf8(role))])) };
                return self.buffer.append(TableRef::new("xt", "role_membership"), StoredEvent { iid, system_from: self.tx_key().system_time, valid_time: interval(self.tx_key().system_time, Timestamp::MAX)?, operation, recency: None });
            }
        };
        let table = table.ok_or_else(|| Anomaly::fault("rtdb.tx/mutation-table", "Mutation has no target"))?;
        if table.database != self.context.database { return Err(Anomaly::incorrect("xtdb/table-not-found", "Mutation cannot write to another database")); }
        check_table(&table.table)?;
        let mut cursor = execute(input, context, arguments, syntax).await?;
        let system_time = self.tx_key().system_time;
        while let Some(batch) = cursor.next_batch().await? {
            if matches!(kind, MutationKind::Put)
                && let Some((documents, headers)) = arrow_mutation(&batch, system_time, &self.context.memory)? {
                self.buffer.put_batch(&table.table, documents.batch().clone(), documents.types().clone(), headers)?;
            } else {
                mutation_events(batch.batch(), kind, system_time, |event| self.buffer.append_to(&table.table, event))?;
            }
        }
        Ok(())
    }

    async fn apply(&mut self, operation: &Operation) -> Result<()> {
        let span = match operation {
            Operation::Public(TxOp::Sql { sql, .. } | TxOp::SqlBytes { sql, .. }) => self.context.transaction_span(self.trace_parent, "xtdb.transaction.sql", &[("query.text", rtdb_plan::TraceValue::Text(sql))]),
            Operation::Public(TxOp::PatchDocs { table, .. }) | Operation::PatchIids { table, .. } => self.context.transaction_span(self.trace_parent, "xtdb.transaction.patch-docs", &[("db", rtdb_plan::TraceValue::Text(&self.context.database)), ("schema", rtdb_plan::TraceValue::Text(&table.schema)), ("table", rtdb_plan::TraceValue::Text(&table.table))]),
            _ => None,
        };
        let parent = self.trace_parent;
        self.trace_parent = span.as_ref().map(|span| span.context()).or(parent);
        let result = self.apply_inner(operation).await;
        self.trace_parent = parent;
        result
    }
    async fn apply_inner(&mut self, operation: &Operation) -> Result<()> {
        self.context.check_cancelled()?;
        match operation {
            Operation::Public(TxOp::Sql { sql, args }) => match args {
                Some(arguments) => {
                    let mut planned = None;
                    for (index, row) in arguments.indexed_rows() { self.execute_sql_typed(sql, Some(row), Some(arguments.types()), &mut planned).await.map_err(|error| error.with_context("arg-idx", index.into()))?; }
                    Ok(())
                }
                None => self.execute_sql(sql, None).await,
            },
            Operation::Public(TxOp::SqlBytes { sql, arg_bytes }) => {
                if arg_bytes.as_slice().is_empty() { return self.execute_sql(sql, None).await; }
                let arguments = decode_arguments(arg_bytes.as_slice(), &self.context.memory)?;
                let mut planned = None;
                for (index, row) in arguments.indexed_rows() { self.execute_sql_typed(sql, Some(row), Some(arguments.types()), &mut planned).await.map_err(|error| error.with_context("arg-idx", index.into()))?; }
                Ok(())
            }
            Operation::Public(TxOp::PutDocs { table, documents, valid_from, valid_to }) => self.put_documents(table, documents, None, *valid_from, *valid_to),
            Operation::PutIids { table, documents, iids, valid_from, valid_to } => self.put_documents(table, documents, Some(iids), *valid_from, *valid_to),
            Operation::Public(TxOp::PutArrowDocs { table, documents, valid_from, valid_to }) =>
                self.put_arrow_documents(table, documents, None, *valid_from, *valid_to),
            Operation::PutArrowIids { table, documents, iids, valid_from, valid_to } =>
                self.put_arrow_documents(table, documents, Some(iids), *valid_from, *valid_to),
            Operation::Public(TxOp::DeleteDocs { table, ids, valid_from, valid_to }) => {
                check_table(table)?;
                let interval = interval(valid_from.unwrap_or(self.tx_key().system_time), valid_to.unwrap_or(Timestamp::MAX))?;
                for id in ids.ids() { self.buffer.delete(table.clone(), id, interval)?; }
                Ok(())
            }
            Operation::Public(TxOp::EraseDocs { table, ids }) => { check_table(table)?; for id in ids.ids() { self.buffer.erase(table.clone(), id)?; } Ok(()) }
            Operation::Public(TxOp::PatchDocs { table, documents, valid_from, valid_to }) => self.patch_documents(table, documents.rows(), None, *valid_from, *valid_to).await,
            Operation::PatchIids { table, documents, iids, valid_from, valid_to } => self.patch_documents(table, documents.rows(), Some(iids), *valid_from, *valid_to).await,
            Operation::Removed(name) => Err(Anomaly::unsupported(if name == "xtql" { "xtdb/xtql-dml-removed" } else { "xtdb/tx-fns-removed" }, if name == "xtql" { "XTQL DML is no longer supported, as of 2.0.0-beta7. Please use SQL DML statements instead - see the release notes for more information." } else { "tx-fns are no longer supported, as of 2.0.0-beta7. Please use ASSERTs and SQL DML statements instead - see the release notes for more information." })),
        }
    }
    async fn patch_documents(&mut self, table: &TableRef, documents: &[StructFields<Value>], iids: Option<&[Iid]>, from: Option<Timestamp>, to: Option<Timestamp>) -> Result<()> {
        use rtdb_plan::{Binder, Expr, Relation, RelationNode, Schema, QualifiedTable, TemporalFilter};
        use rtdb_types::{LogicalType, ScalarType};
        if iids.is_some_and(|iids| iids.len() != documents.len()) { return Err(Anomaly::incorrect("xtdb/invalid-iids", "Document and IID counts differ")); }
        let interval = interval(from.unwrap_or(self.tx_key().system_time), to.unwrap_or(Timestamp::MAX))?;
        let bytes = documents.iter().try_fold(65_536usize, |bytes, document| bytes.checked_add(Value::document_retained_bytes(document)?.checked_mul(8).ok_or_else(overflow)?).ok_or_else(overflow))?;
        let _reservation = self.context.memory.reserve(bytes)?;
        let mut rows = Vec::with_capacity(documents.len());
        let mut document_type = LogicalType::Struct(StructFields::new());
        for (index, document) in documents.iter().enumerate() {
            let iid = match iids { Some(iids) => iids[index], None => crate::source::document_id(document, &self.context.memory)?.iid() };
            let document = Value::Struct(document.clone());
            document_type = document_type.join(&document.logical_type());
            rows.push(vec![Expr::Literal(Value::FixedSizeBinary(iid.0.to_vec())), Expr::Literal(document)]);
        }
        let mut binder = Binder::default();
        let fields = vec![binder.field("_iid", None, LogicalType::Scalar(ScalarType::FixedSizeBinary(16)))?, binder.field("doc", None, document_type)?];
        let input = Arc::new(Relation::new(Schema::new(fields)?, RelationNode::Values { rows })?);
        let context = self.context.query_context(self.snapshot()?, self.tx_key().system_time, &self.options.default_tz, self.trace_parent)?;
        let mutation = rtdb_sql::prepare_patch(QualifiedTable { database: self.context.database.clone(), table: table.clone() }, input, TemporalFilter::FromTo { from: Expr::Literal(time_value(interval.lower())), to: Expr::Literal(time_value(interval.upper())) }, &context.catalog)?;
        self.execute_mutation(mutation, context, None, None).await
    }
    fn put_documents(&mut self, table: &TableRef, documents: &crate::Documents, iids: Option<&[Iid]>, from: Option<Timestamp>, to: Option<Timestamp>) -> Result<()> {
        if iids.is_some_and(|iids| iids.len() != documents.rows().len()) { return Err(Anomaly::incorrect("xtdb/invalid-iids", "Document and IID counts differ")); }
        let is_content = |name: &str| !matches!(name, "_valid_from" | "_valid_to");
        for (index, document) in documents.rows().iter().enumerate() {
            let iid = match iids { Some(iids) => iids[index], None => crate::source::document_id(document, &self.context.memory)?.iid() };
            let default_from = from.unwrap_or(self.tx_key().system_time);
            let default_to = to.unwrap_or(Timestamp::MAX);
            let row_from = document.get("_valid_from").map(|value| timestamp(value, default_from)).transpose()?.unwrap_or(default_from);
            let row_to = document.get("_valid_to").map(|value| timestamp(value, default_to)).transpose()?.unwrap_or(default_to);
            let document = document.iter().filter(|(name, _)| is_content(name)).map(|(name, value)| (name.clone(), value.clone())).collect();
            self.buffer.append_to(table, StoredEvent { iid, system_from: self.tx_key().system_time, valid_time: interval(row_from, row_to)?, operation: StoredOperation::Put(document), recency: None })?;
        }
        if let Some(types) = documents.declared_types().filter(|_| !documents.rows().is_empty()) {
            self.buffer.declare_document_types(table, types.iter().filter(|(name, _)| is_content(name)))?;
        }
        Ok(())
    }
    fn put_arrow_documents(&mut self, table: &TableRef, documents: &crate::ArrowDocuments, iids: Option<&[Iid]>,
        from: Option<Timestamp>, to: Option<Timestamp>) -> Result<()> {
        let count = documents.row_count();
        if iids.is_some_and(|iids| iids.len() != count) {
            return Err(Anomaly::incorrect("xtdb/invalid-iids", "Document and IID counts differ"));
        }
        let valid = interval(from.unwrap_or(self.tx_key().system_time), to.unwrap_or(Timestamp::MAX))?;
        let _scratch = self.context.memory.reserve(count.checked_mul(128).ok_or_else(overflow)?)?;
        let headers = (0..count).map(|row| {
            let iid = match iids { Some(iids) => iids[row], None => documents.iid(row)? };
            Ok((iid, valid))
        }).collect::<Result<Vec<_>>>()?;
        self.buffer.put_batch(table, documents.batch().clone(), documents.types().clone(), headers)?;
        self.buffer.declare_document_types(table, documents.types())
    }

    fn write_result(&mut self, result: &TransactionResult, metadata: Option<&UserMetadata>) -> Result<()> {
        let key = result.tx_key();
        let error = result.encoded_error()?;
        let bound = error.as_ref().map_or(0, |error| error.as_slice().len()).checked_add(metadata.map(|value| value.value().retained_bytes()).transpose()?.unwrap_or(0)).and_then(|bytes| bytes.checked_add(16_384)).ok_or_else(overflow)?;
        let _reservation = self.context.memory.reserve(bound)?;
        let table = TableRef::new("xt", "txs");
        let error_column = "error".to_owned();
        let metadata_column = "user_metadata".to_owned();
        let document = StructFields::from([
            ("_id".into(), Value::I64(key.tx_id.0)), ("system_time".into(), time_value(key.system_time)),
            ("committed".into(), Value::Bool(result.committed())),
            (metadata_column.clone(), metadata.map(|metadata| metadata.value().clone()).unwrap_or(Value::Null)),
            (error_column.clone(), error.map(|error| Value::Transit(error.as_slice().to_vec())).unwrap_or(Value::Null)),
        ]);
        self.buffer.put(table.clone(), document, interval(key.system_time, Timestamp::MAX)?)?;
        self.buffer.declare_document_types(&table, [(&error_column, &LogicalType::Scalar(ScalarType::Transit).maybe()), (&metadata_column, &LogicalType::Struct(StructFields::new()).maybe())])
    }
}

fn db_op_bytes(operation: Option<&DbOp>) -> Result<usize> {
    let encoded = match operation { Some(DbOp::AttachDatabase(value)) => value.encoded_len(), Some(DbOp::DetachDatabase(value)) => value.encoded_len(), None => 0 };
    encoded.checked_mul(4).and_then(|bytes| bytes.checked_add(512)).ok_or_else(overflow)
}

fn admit_sql_syntax(sql: &str, memory: &Arc<MemoryBudget>) -> Result<Arc<MemoryReservation>> {
    memory.reserve(rtdb_sql::statement_syntax_estimate(sql.len()).ok_or_else(overflow)?)
}

/// A mutation planned for one argument row, and what its planning read from the catalog.
struct PlannedMutation { options: rtdb_sql::PrepareOptions, lookups: rtdb_plan::CatalogLookups, reads: bool, mutation: rtdb_sql::Mutation }

/// Whether a mutation's input may read a table: anything but literal rows and the expressions over them may.
fn reads_tables(mutation: &rtdb_sql::Mutation) -> bool {
    fn reads(relation: &rtdb_plan::Relation) -> bool {
        use rtdb_plan::RelationNode as N;
        !matches!(relation.node(), N::Values { .. } | N::LiteralPages { .. } | N::Project { .. } | N::Extend { .. } | N::Rename { .. } | N::Filter { .. })
            || relation.children().into_iter().any(|child| reads(child))
    }
    match mutation {
        rtdb_sql::Mutation::Put { input, .. } | rtdb_sql::Mutation::Delete { input, .. } | rtdb_sql::Mutation::Erase { input, .. } => reads(input),
        _ => true,
    }
}

async fn execute(input: Arc<rtdb_plan::Relation>, context: Arc<QueryContext>, arguments: Option<&[Value]>, syntax: Option<Arc<MemoryReservation>>) -> Result<QueryCursor> {
    let count = input.parameter_count();
    if arguments.is_none() && count != 0 { return Err(Anomaly::incorrect("xtdb.indexer/missing-sql-args", "Arguments list was expected but not provided").with_context("param-count", count.into())); }
    let arguments = arguments.unwrap_or_default();
    if arguments.len() != count { return Err(Anomaly::incorrect("xtdb.indexer/incorrect-sql-arg-count", format!("Parameter error: {} provided, {count} expected", arguments.len())).with_context("param-count", count.into()).with_context("arg-count", arguments.len().into())); }
    let query = PreparedQuery::new_in(input, &context.memory)?;
    drop(syntax);
    query.execute_columns(context, arguments.to_vec()).await
}
fn validate_document_operation(operation: &Operation) -> Result<()> {
    let (table, documents, verb, permitted) = match operation {
        Operation::Public(TxOp::PutDocs { table, documents, .. }) | Operation::PutIids { table, documents, .. } =>
            (table, documents.columns(), "put", &["_id", "_fn", "_valid_from", "_valid_to"][..]),
        Operation::Public(TxOp::PutArrowDocs { table, documents, .. }) | Operation::PutArrowIids { table, documents, .. } =>
            (table, documents.columns(), "put", &["_id"][..]),
        Operation::Public(TxOp::PatchDocs { table, documents, .. }) | Operation::PatchIids { table, documents, .. } =>
            (table, documents.columns(), "patch", &["_id", "_fn"][..]),
        _ => return Ok(()),
    };
    check_table(table)?;
    let forbidden: Vec<_> = documents.iter().map(String::as_str)
        .filter(|name| name.starts_with('_') && !permitted.contains(name)).collect();
    if forbidden.is_empty() { return Ok(()); }
    Err(Anomaly::incorrect("xtdb/forbidden-columns", format!("Cannot {verb} documents with columns: [{}]", forbidden.join(", ")))
        .with_transit_context("table", &T::table_ref(table))?
        .with_context("forbidden-cols", serde_json::json!(forbidden)))
}

/// The table a put, delete or erase writes; the reference's crash-log wrapper around those adds it to any error they raise.
/// Its patch path has no such wrapper.
fn document_table(operation: &Operation) -> Option<&TableRef> {
    match operation {
        Operation::Public(TxOp::PutDocs { table, .. } | TxOp::PutArrowDocs { table, .. } | TxOp::DeleteDocs { table, .. } | TxOp::EraseDocs { table, .. })
        | Operation::PutIids { table, .. } | Operation::PutArrowIids { table, .. } => Some(table),
        _ => None,
    }
}

fn check_table(table: &TableRef) -> Result<()> {
    if is_reserved_write_schema(&table.schema) {
        return Err(Anomaly::incorrect("xtdb/forbidden-table", format!("Cannot write to table: {}", table_name(table)))
            .with_context("table", transit::to_json_value(&T::table_ref(table))?));
    }
    Ok(())
}
fn interval(from: Timestamp, to: Timestamp) -> Result<Interval> {
    if from >= to {
        return Err(Anomaly::incorrect("xtdb.indexer/invalid-valid-times", "Invalid valid times")
            .with_context("valid-from", transit::to_json_value(&T::instant(from.into())?)?)
            .with_context("valid-to", transit::to_json_value(&T::instant(to.into())?)?));
    }
    Interval::new(from, to)
}
#[derive(Clone, Copy)]
enum MutationKind { Put, Delete, Erase }

fn arrow_mutation(batch: &rtdb_columnar::OwnedBatch, system_time: Timestamp,
    memory: &Arc<rtdb_columnar::MemoryBudget>) -> Result<Option<(crate::ArrowDocuments, Vec<(Iid, Interval)>)>> {
    let schema = batch.batch().schema();
    let Some(structs) = batch.batch().column_by_name("doc")
        .and_then(|array| array.as_any().downcast_ref::<arrow_array::StructArray>())
        .filter(|array| array.null_count() == 0) else { return Ok(None); };
    let docs = arrow_array::RecordBatch::from(structs.clone());
    let Some(documents) = crate::ArrowDocuments::from_batch(&docs, memory, Some(batch), false)? else { return Ok(None); };
    let column = |name| schema.index_of(name).map_err(|error| Anomaly::fault("rtdb.tx/mutation-schema", error.to_string()))
        .and_then(|index| OutputColumn::new(schema.field(index), batch.batch().column(index).as_ref()));
    let iids = column("_iid")?;
    let from = column("_valid_from")?;
    let to = column("_valid_to")?;
    let _scratch = memory.reserve(documents.row_count().checked_mul(128).ok_or_else(overflow)?)?;
    let headers = (0..documents.row_count()).map(|row| {
        let valid = interval(from.time(row, system_time)?, to.time(row, Timestamp::MAX)?)?;
        Ok((iids.iid(row)?, valid))
    }).collect::<Result<_>>()?;
    Ok(Some((documents, headers)))
}

/// The events one batch of a mutation's output writes, in its row order.
fn mutation_events(batch: &arrow_array::RecordBatch, kind: MutationKind, system_time: Timestamp, mut append: impl FnMut(StoredEvent) -> Result<()>) -> Result<()> {
    let schema = batch.schema();
    let column = |name| schema.index_of(name).map_err(|error| Anomaly::fault("rtdb.tx/mutation-schema", error.to_string()))
        .and_then(|index| OutputColumn::new(schema.field(index), batch.column(index).as_ref()));
    let iids = column("_iid")?;
    let times = match kind { MutationKind::Erase => None, _ => Some((column("_valid_from")?, column("_valid_to")?)) };
    let documents = match kind { MutationKind::Put => Some(column("doc")?), _ => None };
    for row in 0..batch.num_rows() {
        let iid = iids.iid(row)?;
        let valid_time = match &times { None => Interval::ALL, Some((from, to)) => interval(from.time(row, system_time)?, to.time(row, Timestamp::MAX)?)? };
        let operation = match (&documents, kind) {
            (Some(documents), _) => match documents.read(row)? { Value::Struct(document) => StoredOperation::Put(document), _ => return Err(Anomaly::fault("rtdb.tx/mutation-document", "Put mutation has no document")) },
            (None, MutationKind::Delete) => StoredOperation::Delete,
            (None, _) => StoredOperation::Erase,
        };
        append(StoredEvent { iid, system_from: system_time, valid_time, operation, recency: None })?;
    }
    Ok(())
}

/// One column of a mutation's output, read through its typed array where its layout is the one the event field takes,
/// and cell by cell as `read_value` reads it otherwise.
enum OutputColumn<'a> {
    Iids(&'a arrow_array::FixedSizeBinaryArray),
    Times(&'a arrow_array::TimestampMicrosecondArray),
    Struct { array: &'a arrow_array::StructArray, children: Vec<(&'a String, Option<(LogicalType, &'a arrow_schema::Field)>)> },
    Cells(rtdb_columnar::ColumnReader<'a>, &'a dyn arrow_array::Array),
}

impl<'a> OutputColumn<'a> {
    fn new(field: &'a arrow_schema::Field, array: &'a dyn arrow_array::Array) -> Result<Self> {
        use arrow_schema::DataType;
        let reader = rtdb_columnar::ColumnReader::new(field)?;
        let logical = match reader.logical() { LogicalType::Maybe(inner) => inner.as_ref(), logical => logical };
        let any = array.as_any();
        Ok(match (logical, field.data_type()) {
            (LogicalType::Scalar(ScalarType::FixedSizeBinary(16)), DataType::FixedSizeBinary(16)) => match any.downcast_ref() { Some(array) => Self::Iids(array), None => Self::Cells(reader, array) },
            (LogicalType::Scalar(ScalarType::Timestamp { unit: TimeUnit::Microsecond, timezone: Some(_) }), DataType::Timestamp(arrow_schema::TimeUnit::Microsecond, _)) => match any.downcast_ref() { Some(array) => Self::Times(array), None => Self::Cells(reader, array) },
            (LogicalType::Struct(types), DataType::Struct(fields)) => {
                let children = fields.iter().map(|child| match types.get(child.name()) {
                    Some(LogicalType::Nothing) => Some((child.name(), None)),
                    Some(logical) => Some((child.name(), Some((logical.clone(), child.as_ref())))),
                    None => None,
                }).collect::<Option<Vec<_>>>();
                match (children, any.downcast_ref::<arrow_array::StructArray>()) { (Some(children), Some(structs)) => Self::Struct { array: structs, children }, _ => Self::Cells(reader, array) }
            }
            _ => Self::Cells(reader, array),
        })
    }
    fn read(&self, row: usize) -> Result<Value> {
        match self {
            Self::Struct { array, children } => {
                if row >= array.len() { return Err(Anomaly::incorrect("rtdb/row-index", "Cell index exceeds array length")); }
                if array.is_null(row) { return Ok(Value::Null); }
                let mut values = StructFields::new();
                for (index, (name, child)) in children.iter().enumerate() {
                    let value = match child { None => Value::Undefined, Some((logical, field)) => rtdb_columnar::read_value_as(logical, field, array.column(index).as_ref(), row)? };
                    values.insert((*name).clone(), value);
                }
                Ok(Value::Struct(values))
            }
            Self::Cells(reader, array) => reader.read(*array, row),
            Self::Iids(array) => Ok(if array.is_null(row) { Value::Null } else { Value::FixedSizeBinary(array.value(row).to_vec()) }),
            Self::Times(array) => Ok(if array.is_null(row) { Value::Null } else { time_value(Timestamp(array.value(row))) }),
        }
    }
    fn iid(&self, row: usize) -> Result<Iid> {
        let invalid = || Anomaly::incorrect("xtdb/invalid-id", "Mutation IID is null or invalid");
        match self {
            Self::Iids(array) => if array.is_null(row) { Err(invalid()) } else { Iid::from_slice(array.value(row)) },
            column => match column.read(row)? { Value::FixedSizeBinary(bytes) => Iid::from_slice(&bytes), _ => Err(invalid()) },
        }
    }
    fn time(&self, row: usize, fallback: Timestamp) -> Result<Timestamp> {
        match self {
            Self::Times(array) => Ok(if array.is_null(row) { fallback } else { Timestamp(array.value(row)) }),
            column => timestamp(&column.read(row)?, fallback),
        }
    }
}

fn timestamp(value: &Value, fallback: Timestamp) -> Result<Timestamp> {
    match value { Value::Null | Value::Undefined => Ok(fallback), Value::Timestamp { value, unit: TimeUnit::Microsecond, timezone: Some(_) } => Ok(Timestamp(*value)), _ => Err(Anomaly::fault("xtdb/invalid-timestamp-col-type", "Invalid timestamp col type")) }
}
fn time_value(time: Timestamp) -> Value { Value::Timestamp { value: time.0, unit: TimeUnit::Microsecond, timezone: Some("UTC".into()) } }
fn tx_key_context(key: TransactionKey) -> Result<serde_json::Value> {
    transit::to_json_value(&T::Tagged { tag: "xtdb/tx-key".into(), representation: Box::new(T::Map(vec![
        (T::Keyword("tx-id".into()), T::Integer(key.tx_id.0)),
        (T::Keyword("system-time".into()), T::instant(key.system_time.into())?),
    ])) })
}

fn overflow() -> Anomaly { Anomaly::fault("rtdb.tx/overflow", "Transaction position or allocation exceeded its range") }

#[cfg(test)]
mod tests {
    use super::*;

    struct NoRoleChecks;
    impl TransactionAuthorization for NoRoleChecks {
        fn check_role_membership(&self, _: &str, _: Option<&str>) -> Result<()> { panic!("unexpected role check") }
    }

    #[tokio::test]
    async fn mutation_syntax_is_charged_until_the_plan_takes_ownership() {
        let memory = MemoryBudget::new(1 << 24);
        let resolve = ResolveContext {
            database: "xtdb".into(), storage: Arc::new(BufferPool::new(Arc::new(rtdb_storage::MemoryStore::default()), 1 << 20)),
            memory: Arc::clone(&memory), query_options: QueryOptions { batch_rows: 1, ..Default::default() },
            scan_accelerators: Default::default(),
            cancellation: CancellationToken::new(), default_tz: "UTC".into(), authorization: Arc::new(NoRoleChecks),
            execution_observer: None, tracer: None,
        };
        let state = rtdb_index::PartitionState::empty(Default::default()).unwrap();
        let options = SubmitOptions::default();
        let mut tx = OpenTransaction::new(state.value().snapshot(), TransactionKey { tx_id: TxId(0), system_time: Timestamp(100) }, &options, &resolve).unwrap();
        let baseline = memory.used_bytes();

        let malformed = "DELETE FROM docs WHERE @";
        let syntax_bytes = rtdb_sql::statement_syntax_estimate(malformed.len()).unwrap();
        let lexical_error = rtdb_sql::parse_in(malformed, &memory).unwrap_err().with_context("sql", malformed.into());
        assert_eq!(lexical_error.code, "xtdb/sql-error");
        for (headroom, code) in [(syntax_bytes - 1, "rtdb/memory-budget-exceeded"), (syntax_bytes, "xtdb/sql-error")] {
            let held = memory.reserve(memory.limit_bytes() - baseline - headroom).unwrap();
            let error = tx.execute_sql(malformed, None).await.unwrap_err();
            assert_eq!(error.code, code);
            assert_eq!(error.context["sql"], malformed);
            if headroom == syntax_bytes { assert_eq!(error, lexical_error); }
            assert_eq!(memory.used_bytes(), baseline + held.bytes());
        }

        let source = format!("ASSERT TRUE /*{}*/", "x".repeat(4096));
        let syntax_bytes = rtdb_sql::statement_syntax_estimate(source.len()).unwrap();
        let probe = MemoryBudget::new(1 << 20);
        let context = resolve.query_context(tx.snapshot().unwrap(), tx.tx_key().system_time, "UTC", None).unwrap();
        let raw = rtdb_sql::prepare_tx_in(&source, &context.catalog, &Default::default(), &probe).unwrap();
        let rtdb_sql::Mutation::Assert { input, .. } = raw else { panic!("expected ASSERT relation"); };
        assert_eq!(probe.used_bytes(), 0);
        let query = PreparedQuery::new_in(input, &probe).unwrap();
        let plan_bytes = probe.used_bytes();
        assert!(plan_bytes > 0);
        drop((query, context));
        assert_eq!(probe.used_bytes(), 0);
        assert_eq!(memory.used_bytes(), baseline);

        let headroom = syntax_bytes + plan_bytes;
        let short = memory.reserve(memory.limit_bytes() - baseline - headroom + 1).unwrap();
        let error = tx.execute_sql(&source, None).await.unwrap_err();
        let retained = baseline + short.bytes() + syntax_bytes;
        let expected = format!("Cannot reserve {plan_bytes} bytes from budget {} with {retained} retained", memory.limit_bytes());
        assert_eq!(error.code, "rtdb/memory-budget-exceeded");
        assert_eq!(error.message.as_deref(), Some(expected.as_str()));
        assert_eq!(error.context["sql"], source.as_str());
        assert_eq!(memory.used_bytes(), baseline + short.bytes());
        drop(short);

        let exact = memory.reserve(memory.limit_bytes() - baseline - headroom).unwrap();
        tx.execute_sql(&source, None).await.unwrap();
        assert_eq!(memory.used_bytes(), baseline + exact.bytes());
        drop(exact);
        let missing = tx.execute_sql("INSERT INTO docs (_id) VALUES (?)", None).await.unwrap_err();
        assert_eq!(missing.code, "xtdb.indexer/missing-sql-args");
        assert_eq!(memory.used_bytes(), baseline);
        drop(tx);
        assert_eq!(memory.used_bytes(), 0);
    }

    #[test]
    fn reserved_tables_in_resolution_retain_the_table_reference() {
        let table = TableRef::new("information_schema", "columns");
        let error = check_table(&table).unwrap_err();
        assert_eq!(error.code, "xtdb/forbidden-table");
        assert_eq!(error.message.as_deref(), Some("Cannot write to table: information_schema/columns"));
        assert_eq!(transit::from_json_value(&error.context["table"]).unwrap(), T::table_ref(&table));
        assert!(check_table(&TableRef::new("public", "columns")).is_ok());
    }

    #[test]
    fn error_transaction_keys_preserve_jvm_instant_text() {
        for (micros, text) in [(0, "1970-01-01T00:00:00Z"), (-1, "1969-12-31T23:59:59.999999Z"), (1000, "1970-01-01T00:00:00.001Z"), (946_684_800_000_000, "2000-01-01T00:00:00Z")] {
            assert_eq!(T::instant(Timestamp(micros).into()).unwrap(), T::Tagged { tag: "time/instant".into(), representation: Box::new(T::String(text.into())) });
        }
    }

    #[test]
    fn invalid_valid_time_errors_roundtrip_with_instant_bounds() {
        let error = interval(Timestamp(0), Timestamp(-1)).unwrap_err();
        assert_eq!(error.code, "xtdb.indexer/invalid-valid-times");
        let durable = transit::decode_anomaly(&transit::encode_anomaly(&error).unwrap()).unwrap();
        for (field, text) in [("valid-from", "1970-01-01T00:00:00Z"), ("valid-to", "1969-12-31T23:59:59.999999Z")] {
            assert_eq!(transit::from_json_value(&durable.context[field]).unwrap(), T::Tagged { tag: "time/instant".into(), representation: Box::new(T::String(text.into())) });
        }
    }

    /// The events a mutation's output writes when every cell is read through `read_value`, as each row once was.
    async fn events_read_row_by_row(mut cursor: QueryCursor, kind: MutationKind, system_time: Timestamp) -> Vec<StoredEvent> {
        let mut events = Vec::new();
        while let Some(batch) = cursor.next_batch().await.unwrap() {
            let (batch, schema) = (batch.batch(), batch.batch().schema());
            let cell = |name: &str, row| { let index = schema.index_of(name).unwrap(); rtdb_columnar::read_value(schema.field(index), batch.column(index).as_ref(), row).unwrap() };
            for row in 0..batch.num_rows() {
                let Value::FixedSizeBinary(iid) = cell("_iid", row) else { panic!("an IID") };
                let valid_time = match kind { MutationKind::Erase => Interval::ALL, _ => interval(timestamp(&cell("_valid_from", row), system_time).unwrap(), timestamp(&cell("_valid_to", row), Timestamp::MAX).unwrap()).unwrap() };
                let operation = match kind { MutationKind::Put => { let Value::Struct(document) = cell("doc", row) else { panic!("a document") }; StoredOperation::Put(document) }, MutationKind::Delete => StoredOperation::Delete, MutationKind::Erase => StoredOperation::Erase };
                events.push(StoredEvent { iid: Iid::from_slice(&iid).unwrap(), system_from: system_time, valid_time, operation, recency: None });
            }
        }
        events
    }

    #[tokio::test]
    async fn a_mutation_read_a_column_at_a_time_writes_the_events_a_row_by_row_read_does() {
        let memory = MemoryBudget::new(1 << 26);
        let resolve = ResolveContext {
            database: "xtdb".into(), storage: Arc::new(BufferPool::new(Arc::new(rtdb_storage::MemoryStore::default()), 1 << 20)),
            memory: Arc::clone(&memory), query_options: QueryOptions::default(),
            scan_accelerators: Default::default(),
            cancellation: CancellationToken::new(), default_tz: "UTC".into(), authorization: Arc::new(NoRoleChecks),
            execution_observer: None, tracer: None,
        };
        let state = rtdb_index::PartitionState::empty(Default::default()).unwrap();
        let options = SubmitOptions::default();
        let system_time = Timestamp(1_000_000);
        let mut tx = OpenTransaction::new(state.value().snapshot(), TransactionKey { tx_id: TxId(0), system_time }, &options, &resolve).unwrap();
        let table = TableRef::new("public", "items");
        let instant = |micros: i64| Value::Timestamp { value: micros, unit: TimeUnit::Microsecond, timezone: Some("UTC".into()) };
        let cells: [(Value, Value, Value, Value, Value); 5] = [
            (Value::F64(f64::NAN), Value::Utf8("a".into()), Value::I64(1), instant(1_000), Value::DateDay(18_000)),
            (Value::F64(1.5), Value::Null, Value::Null, Value::Null, Value::Null),
            (Value::F64(-0.0), Value::Utf8(String::new()), Value::I64(3), instant(-1), Value::DateDay(-1)),
            (Value::Null, Value::Utf8("b".into()), Value::I64(200), instant(i64::MAX - 1), Value::DateDay(0)),
            (Value::F64(0.0), Value::Utf8("c".into()), Value::I64(-5), instant(0), Value::Null),
        ];
        for (id, (f, t, n, ts, d)) in cells.into_iter().enumerate() {
            let document = StructFields::from([("_id".into(), Value::I64(id as i64)), ("f".into(), f), ("t".into(), t), ("n".into(), n), ("ts".into(), ts), ("d".into(), d)]);
            let to = if id == 2 { Timestamp(5_000_000) } else { Timestamp::MAX };
            tx.buffer.put(table.clone(), document, Interval::new(Timestamp(id as i64 * 1000), to).unwrap()).unwrap();
        }
        let statements = [
            "UPDATE items SET f = f", "UPDATE items SET f = f * 1.01 WHERE n < 100", "UPDATE items SET t = t, ts = ts, d = d",
            "UPDATE items SET n = n + 1", "UPDATE items SET t = 'z' WHERE _id = 1", "UPDATE items SET d = d, f = 0.0 WHERE ts IS NOT NULL",
            "UPDATE items SET ts = NULL", "DELETE FROM items WHERE n = 1", "INSERT INTO copies SELECT * FROM items",
        ];
        let mut written = 0;
        for statement in statements {
            let context = || resolve.query_context(tx.snapshot().unwrap(), system_time, "UTC", None).unwrap();
            let (input, kind) = match rtdb_sql::prepare_tx(statement, &context().catalog, &Default::default()).unwrap() {
                rtdb_sql::Mutation::Put { input, .. } => (input, MutationKind::Put),
                rtdb_sql::Mutation::Delete { input, .. } => (input, MutationKind::Delete),
                _ => panic!("{statement} is a put or a delete"),
            };
            let query = PreparedQuery::new_in(input, &memory).unwrap();
            let expected = events_read_row_by_row(query.execute(context(), vec![]).await.unwrap(), kind, system_time).await;
            let mut cursor = query.execute_columns(context(), vec![]).await.unwrap();
            let mut actual = Vec::new();
            while let Some(batch) = cursor.next_batch().await.unwrap() { mutation_events(batch.batch(), kind, system_time, |event| { actual.push(event); Ok(()) }).unwrap(); }
            assert_eq!(format!("{actual:?}"), format!("{expected:?}"), "{statement}");
            written += actual.len();
        }
        assert!(written > 15, "the statements should write events, wrote {written}");
    }
}

/// A search index's name keys the object paths of its search files.
fn check_search_index_name(name: &str) -> Result<()> {
    if name.is_empty() || matches!(name, "." | "..") || name.contains(['/', '\\', '\0', '~']) {
        return Err(Anomaly::incorrect("xtdb/invalid-search-index-name", format!("Invalid search index name: {name}")));
    }
    Ok(())
}

fn check_ordered_index_name(name: &str) -> Result<()> {
    if name.is_empty() {
        return Err(Anomaly::incorrect("xtdb/invalid-index-name", "An ordered index name cannot be empty"));
    }
    Ok(())
}
