use rtdb_types::StructFields;
use std::collections::{BTreeMap, BTreeSet, VecDeque};
use std::num::NonZeroU32;
use std::sync::Arc;

use async_trait::async_trait;
use prost::Message;
use arrow_array::{Array, RecordBatch, cast::AsArray};
use arrow_schema::{DataType, Schema};
use rtdb_columnar::{ArrowCell, MemoryBudget, MemoryReservation, OwnedBatch};
use rtdb_format::{envelope::{ReplicaBody, ReplicaEnvelope}, data::{StoredEvent, StoredOperation}, generated::xtdb::log::proto::{AcceptanceStage, ResolvedTx as WireResolvedTx, resolved_tx::DbOp}};
use rtdb_format::transit::{self, TransitValue as T};
use rtdb_index::{Snapshot, StagedWrite, TransactionBuffer};
use rtdb_query::{CatalogSnapshot, DatabaseSnapshot, PreparedQuery, QueryContext, QueryCursor, QueryOptions};
use rtdb_storage::cache::BufferPool;
use rtdb_types::{Anomaly, AnomalyCategory, EntityId, EventHeader, EventOp, Iid, Interval, LogicalType, PLACED_ATTEMPT_OUTCOME_TABLE, PlacedAttempt, Result, ScalarType, SourcePosition, TableRef, TimeUnit, Timestamp, TransactionKey, TxId, Value};
use tokio_util::sync::CancellationToken;

use crate::bounded_retry::is_bounded_receipts_table;
use crate::replay_submission::{ReplaySubmission, duplicate_replay, is_replay_table, replay_table};
use crate::system_time::{SourceTime, TimeRejection};
use crate::values::{BoundArgumentRow, IngestMode, plain_scalar_field};
use crate::{ArrowDocuments, SourceTransaction, SubmitOptions, TransactionResult, TxOp, UserMetadata, source::{Operation, SourceGuard, decode_arguments, is_reserved_write_schema, table_name}};

mod hosted_control;
mod constraint_receipt;
mod retry;
mod retention;
pub use retention::installed_receipt_time;
mod fixed_range;
mod owned_region;
pub(crate) mod ordered_micros;
mod replay_constraints;
mod replay_worlds;
mod owned_capacity;
use crate::hosted_control::HOSTED_CAPACITY_DECLARATIONS;
use owned_capacity::OwnedCapacityChanges;
mod owned_unique;
use owned_unique::OwnedUniqueChanges;
mod world_constraints;
pub use world_constraints::{ArchivedWorldConstraints, WorldConstraintArchive};

pub const IDEMPOTENCY_GUARD_RESULT: &str = "rtdb/idempotency-guard";

pub trait TransactionAuthorization: Send + Sync {
    /// Rejects an absent principal or a principal without the reference superuser authority.
    fn check_role_membership(&self, database: &str, principal: Option<&str>) -> Result<()>;
    /// Identifies the configured principal entitled to order hosted control effects.
    fn is_hosted_control_owner(&self, _principal: &str) -> bool { false }
}

pub struct ResolveContext {
    pub write_event_origins: bool,
    pub placed_receipt_retirement: bool,
    pub owned_witness_scope: Option<rtdb_types::PlacedAttemptScope>,
    pub world_constraints: Option<Arc<dyn WorldConstraintArchive>>,
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

pub struct OwnedReplicaBody { body: ReplicaBody, _reservation: Arc<MemoryReservation>, _table_reservations: Vec<Arc<MemoryReservation>> }
impl OwnedReplicaBody {
    pub fn body(&self) -> &ReplicaBody { &self.body }
    fn acceptance_stage(&self) -> Option<&AcceptanceStage> {
        let ReplicaBody::ResolvedTx(tx) = &self.body else { return None; };
        tx.acceptance_stage.as_ref()
    }
}

#[derive(Clone, Copy)]
enum ResolvedInput {
    Position(SourcePosition),
    Placed { attempt: PlacedAttempt, retirement: bool },
}

impl ResolvedInput {
    fn position(self) -> SourcePosition {
        match self { Self::Position(position) => position, Self::Placed { attempt, .. } => attempt.position() }
    }
}

pub struct ResolvedTx {
    writes: Arc<StagedWrite>,
    result: TransactionResult,
    input: ResolvedInput,
    external_token: Option<Vec<u8>>,
    db_op: Option<DbOp>,
    inline_replica: Option<OwnedReplicaBody>,
    _reservation: Arc<MemoryReservation>,
}

impl ResolvedTx {
    fn new(writes: StagedWrite, result: TransactionResult, input: ResolvedInput, token: Option<Vec<u8>>, db_op: Option<DbOp>, context: &ResolveContext) -> Result<Self> {
        let bound = 4096usize.checked_add(token.as_ref().map_or(0, Vec::capacity)).and_then(|bytes| bytes.checked_add(db_op_bytes(db_op.as_ref()).ok()?)).and_then(|bytes| bytes.checked_add(result.error().map(crate::values::anomaly_bytes).transpose().ok()?.unwrap_or(0))).ok_or_else(overflow)?;
        let reservation = context.memory.reserve(bound)?;
        let writes = if context.write_event_origins && result.committed() && writes.row_count()? > 0 { writes.assign_event_origins(&context.memory)? } else { writes };
        Ok(Self { writes: Arc::new(writes), result, input, external_token: token, db_op, inline_replica: None, _reservation: reservation })
    }

    pub fn writes(&self) -> &Arc<StagedWrite> { &self.writes }
    pub fn result(&self) -> &TransactionResult { &self.result }
    pub fn source_position(&self) -> SourcePosition { self.input.position() }
    pub fn placed_attempt(&self) -> Option<PlacedAttempt> {
        match self.input { ResolvedInput::Placed { attempt, .. } => Some(attempt), ResolvedInput::Position(_) => None }
    }
    pub fn external_token(&self) -> Option<&[u8]> { self.external_token.as_deref() }
    pub fn db_op(&self) -> Option<&DbOp> { self.db_op.as_ref() }
    pub fn inline_replica_body(&self) -> Option<&ReplicaBody> { self.inline_replica.as_ref().map(OwnedReplicaBody::body) }
    pub fn staging_clock_upper(&self) -> Option<Timestamp> {
        self.inline_replica.as_ref()?.acceptance_stage()?.clock_upper_micros.map(Timestamp)
    }

    fn prepare_inline_replica(&mut self, writer: Option<&WriterPublication<'_>>, until: Timestamp, journal_before: Option<rtdb_types::TimedJournalCutoff>, memory: &Arc<MemoryBudget>) -> Result<()> {
        let (writer, maximum) = if journal_before.is_some() {
            writer.and_then(|writer| writer.maximum_timed_original_bytes.map(|maximum| (writer, maximum)))
                .ok_or_else(|| Anomaly::unsupported(
                    "rtdb.owned/timed-original-quorum-unavailable",
                    "The installed journal cannot enforce the original quorum cutoff",
                ))?
        } else {
            writer.and_then(|writer| writer.maximum_inline_bytes.map(|maximum| (writer, maximum)))
                .ok_or_else(|| Anomaly::unsupported("rtdb.owned/replica-inline-unavailable", "The writer has no installed inline replica limit"))?
        };
        // The sample uses a fixed-width field. Reserving it before the
        // capacity check makes substituting the live sample size invariant.
        // The sealed journal cutoff and certificate digest are the only source
        // for the guarded append, so the frame cannot name other clock evidence.
        let mut body = self.encode_replica_body(memory, Some(AcceptanceStage {
            clock_upper_micros: Some(0), cutoff_micros: Some(until.0),
            journal_cutoff_micros: journal_before.map(|cutoff| cutoff.before().0),
            clock_certificate_sha256: journal_before.map(|cutoff| cutoff.certificate_sha256().to_vec()),
        }))?;
        if ReplicaEnvelope::maximum_encoded_len(body.body()) > maximum {
            return Err(Anomaly::incorrect("rtdb.owned/replica-inline-bound", "The resolved transaction exceeds the writer's inline replica limit"));
        }
        // The effect and receipt are encoded, and the candidate can still be discarded.
        // Round up the sampled clock because its microsecond coordinate was truncated.
        let Some(upper) = (writer.clock)().0.checked_add(1).filter(|upper| 0 < *upper && *upper < until.0) else {
            return Err(Anomaly::new(AnomalyCategory::Conflict, "rtdb.owned/expired", "Owned transaction reached its deadline before staging"));
        };
        let ReplicaBody::ResolvedTx(WireResolvedTx { acceptance_stage: Some(stage), .. }) = &mut body.body else {
            return Err(Anomaly::fault("rtdb.tx/acceptance-stage", "Prepared replica body has no acceptance stage"));
        };
        stage.clock_upper_micros = Some(upper);
        self.inline_replica = Some(body);
        Ok(())
    }

    /// Serializes only this transaction's deltas, including zero-row schema declarations.
    /// The append owner supplies the term and sequence after obtaining this body.
    pub fn to_replica_body(&self, memory: &Arc<MemoryBudget>) -> Result<OwnedReplicaBody> {
        let stage = self.inline_replica.as_ref().and_then(OwnedReplicaBody::acceptance_stage).cloned();
        self.encode_replica_body(memory, stage)
    }

    fn encode_replica_body(&self, memory: &Arc<MemoryBudget>, acceptance_stage: Option<AcceptanceStage>) -> Result<OwnedReplicaBody> {
        let mut tables = Vec::new();
        let mut table_reservations = Vec::new();
        let mut bound = 65_536usize.checked_add(db_op_bytes(self.db_op.as_ref())?).ok_or_else(overflow)?;
        for (table, write) in self.writes.tables() {
            let batch = write.batch().ok_or_else(|| Anomaly::fault("rtdb.tx/missing-table-batch", "Resolved table declaration has no Arrow batch"))?;
            let bytes = crate::ipc::encode(batch.arrow().schema().as_ref(), [batch.arrow()], memory)?;
            let name = format!("{}/{}", table.schema, table.table);
            bound = bound.checked_add(name.capacity()).and_then(|bound| bound.checked_add(512)).ok_or_else(overflow)?;
            let (bytes, reservation) = bytes.into_parts();
            tables.push((name, bytes));
            table_reservations.push(reservation);
        }
        let error = self.result.encoded_error()?;
        bound = bound.checked_add(error.as_ref().map_or(0, |error| error.as_slice().len())).and_then(|bound| bound.checked_add(self.external_token.as_ref().map_or(0, Vec::capacity))).ok_or_else(overflow)?;
        let reservation = memory.reserve(bound)?;
        let key = self.result.tx_key();
        let placed_attempt = match self.input {
            ResolvedInput::Placed { attempt, retirement } => Some(rtdb_format::envelope::encode_replica_outcome_attempt(attempt, retirement)),
            ResolvedInput::Position(_) => None,
        };
        let body = ReplicaBody::ResolvedTx(WireResolvedTx {
            tx_id: Some(key.tx_id.0), system_time_micros: Some(key.system_time.0), committed: Some(self.result.committed()), error: error.map(|error| error.as_slice().to_vec()),
            table_data: tables.into_iter().collect(),
            db_op: self.db_op.clone(), external_source_token: self.external_token.clone(), src_msg_id: Some(self.source_position().0),
            placed_attempt, acceptance_stage, search_delta: None,
        });
        Ok(OwnedReplicaBody { body, _reservation: reservation, _table_reservations: table_reservations })
    }
}

pub struct WriterPublication<'a> {
    pub clock: &'a (dyn Fn() -> Timestamp + Send + Sync),
    pub maximum_inline_bytes: Option<usize>,
    pub maximum_timed_original_bytes: Option<usize>,
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
    pub async fn resolve_skipped(&mut self, durable: Snapshot, position: SourcePosition, log_time: Timestamp, attempt: Option<PlacedAttempt>, context: &ResolveContext) -> Result<Arc<ResolvedTx>> {
        let _span = context.transaction_span(None, "xtdb.transaction", &[("operations.count", rtdb_plan::TraceValue::Unsigned(0))]);
        context.check_cancelled()?;
        let durable = rtdb_query::retain_catalog_snapshot(durable, &context.storage).await?;
        let base = self.resolving_snapshot(durable)?;
        let key = TransactionKey { tx_id: TxId(position.0), system_time: self.smooth(log_time)? };
        let options = SubmitOptions { default_tz: context.default_tz.clone(), ..SubmitOptions::default() };
        let mut tx = OpenTransaction::new(base, key, &options, context)?;
        let error = crate::RetainedAnomaly::new(Anomaly::fault("xtdb/skipped-tx", "Transaction was skipped"), &context.memory)?;
        let result = TransactionResult::Aborted { tx_key: key, error: Some(error) };
        let clock = tx.observe_retention_clock(log_time).await?;
        let receipt_time = clock.time;
        tx.retire_expired_receipts(clock).await?;
        tx.write_result(&result, None, log_time, receipt_time)?;
        if let Some(attempt) = attempt {
            tx.retire_placed_outcomes(clock).await?;
            tx.write_attempt_outcome(attempt, log_time, Default::default(), Vec::new(), false)?;
        }
        let input = attempt.map_or(ResolvedInput::Position(position), |attempt| ResolvedInput::Placed {
            attempt, retirement: context.placed_receipt_retirement,
        });
        self.stage(tx.buffer.stage()?, result, input, None, None, context)
    }

    pub async fn resolve_source(&mut self, durable: Snapshot, position: SourcePosition, log_time: Timestamp, request: SourceTransaction, context: &ResolveContext) -> Result<Arc<ResolvedTx>> {
        self.resolve_source_for_writer(durable, position, log_time, request, context, None).await
    }

    pub async fn resolve_source_for_writer(&mut self, durable: Snapshot, position: SourcePosition, log_time: Timestamp, mut request: SourceTransaction, context: &ResolveContext,
        writer: Option<WriterPublication<'_>>,
    ) -> Result<Arc<ResolvedTx>> {
        let _sample = rtdb_plan::ExecutionSample::new(context.execution_observer.clone(), rtdb_plan::ExecutionTimer::TxOp);
        let span = context.transaction_span(None, "xtdb.transaction", &[("operations.count", rtdb_plan::TraceValue::Unsigned(request.operations.len() as u64))]);
        context.check_cancelled()?;
        if request.uses_staging_deadline() && !self.queue.is_empty() {
            return Err(Anomaly::fault("rtdb.tx/strict-predecessor", "A strict source transaction requires a durable predecessor"));
        }
        let durable = rtdb_query::retain_catalog_snapshot(durable, &context.storage).await?;
        let base = self.resolving_snapshot(durable)?;
        let default_time = self.smooth(log_time)?;
        let has_owned_operations = request.operations.iter().any(Operation::is_owned);
        let attempt = request.placed_attempt(position)?;
        let slot = request.operations.iter().find_map(|operation| match operation {
            Operation::Public(TxOp::OwnedClockSlot { predecessor }) => Some(*predecessor),
            _ => None,
        });
        let slot_time = slot.map(|predecessor| {
            let mut count = 0;
            request.operations.retain(|operation| {
                let is_slot = matches!(operation, Operation::Public(TxOp::OwnedClockSlot { .. }));
                count += usize::from(is_slot);
                !is_slot
            });
            if count != 1 || request.options.system_time.is_some() || request.guard.is_some()
                || request.idempotency_key().is_some() || attempt.is_none()
                || attempt.map(PlacedAttempt::scope) != context.owned_witness_scope
                || !request.operations.iter().any(|operation| matches!(operation,
                    Operation::Public(TxOp::OwnedDeadline { .. })))
            {
                return Err(Anomaly::incorrect("rtdb.owned/invalid-clock-slot", "A clock slot requires a fresh placed transaction with a deadline"));
            }
            crate::system_time::clock_slot_time(predecessor, self.resolved_head, log_time)
                .ok_or_else(|| Anomaly::incorrect("rtdb.owned/clock-slot-missed", "The offered transaction clock is unavailable"))
        }).transpose();
        let clock_request = match request.options.system_time {
            None => SourceTime::Automatic,
            Some(requested) if has_owned_operations => SourceTime::ObservedAppend(requested.0),
            Some(requested) => SourceTime::ExactSystem {
                requested: requested.0,
                require_current_append: request.options.user_metadata.as_ref().is_some_and(UserMetadata::requires_ordered_system_time),
            },
        };
        let selected_time = clock_request.select(self.previous_system_time(), log_time.0, default_time.0);
        let key = TransactionKey {
            tx_id: TxId(position.0),
            system_time: match &slot_time {
                Ok(Some(reading)) => *reading,
                Ok(None) => Timestamp(selected_time.as_ref().copied().unwrap_or(default_time.0)),
                Err(_) => default_time,
            },
        };
        let placed_periods = request.guard.is_none() && request.idempotency_key().is_none()
            && attempt.map(PlacedAttempt::scope) == context.owned_witness_scope;
        let placement = if selected_time.is_ok() && slot_time.is_ok() {
            request.place_owned_periods(key.system_time, placed_periods)
                .and_then(|()| request.place_proposal_keys(key.system_time, placed_periods))
        } else {
            Ok(())
        };
        let mut tx = OpenTransaction::new(base.clone(), key, &request.options, context)?;
        tx.trace_parent = span.as_ref().map(|span| span.context());
        let replay = match &request.guard {
            Some(SourceGuard::Replay(replay)) => Some(replay),
            _ => None,
        };
        let clock = tx.observe_retention_clock(log_time).await?;
        let receipt_time = clock.time;
        let bounded_receipt = if let Some(SourceGuard::Bounded(retry)) = &request.guard {
            let scope = retry::RetryScope::new(attempt.map(PlacedAttempt::scope));
            tx.admit_bounded_retry(&scope, &request, *retry, receipt_time).await?
        } else {
            Ok(None)
        };
        let (bounded_receipt, retry_error) = match bounded_receipt {
            Ok(receipt) => (receipt, None),
            Err(error) => (None, Some(error)),
        };
        let plan = constraint_receipt::split(
            &request.operations,
            attempt.is_some() && request.guard.is_none() && request.idempotency_key().is_none(),
        );
        let (primary, refusal, plan_error) = match plan {
            Ok((primary, refusal)) => (primary, refusal, None),
            Err(error) => (request.operations.as_slice(), &[][..], Some(error)),
        };
        let staging_deadline = primary.iter().find_map(|operation| match operation {
            Operation::Public(TxOp::OwnedDeadline { until, recheck_at_staging: true, journal_before }) => Some((*until, *journal_before)),
            _ => None,
        });
        let mut constraint_failed = false;
        let mut outcome = if let Some(error) = slot_time.err().or(placement.err()).or(plan_error).or(retry_error) {
            Err(error)
        } else if has_owned_operations && matches!(request.guard, Some(SourceGuard::Bounded(_))) {
            Err(Anomaly::incorrect("rtdb/idempotency-owned-scope", "Bounded application retry metadata is supported on ordinary transactions"))
        } else if has_owned_operations
            && !request.operations.iter().all(Operation::is_owned)
        {
            Err(Anomaly::incorrect("rtdb.owned/mixed-transaction", "Owned and ordinary writes require separate transactions"))
        } else if let Err(rejection) = selected_time {
            let (requested, error) = match rejection {
                TimeRejection::BehindHistory(requested) => (requested,
                    Anomaly::incorrect("invalid-system-time", "specified system-time must be later than current tx")
                        .with_context("latest-completed-tx", tx_key_context(self.resolved_head.expect("a nonadvancing time has a predecessor"))?)),
                TimeRejection::AheadOfAppend(requested) => (requested,
                    Anomaly::incorrect("rtdb/future-system-time", "specified system-time is later than the source-log timestamp")
                        .with_context("log-time", transit::to_json_value(&T::instant(log_time.into())?)?)),
                TimeRejection::BehindAppend(requested) => (requested,
                    Anomaly::incorrect("rtdb/ordered-system-time", "specified system-time precedes the source-log timestamp")
                        .with_context("log-time", transit::to_json_value(&T::instant(log_time.into())?)?)),
            };
            Err(error.with_context("tx-key", tx_key_context(TransactionKey { tx_id: key.tx_id, system_time: Timestamp(requested) })?))
        } else if staging_deadline.is_some() && (attempt.is_none() || attempt.map(PlacedAttempt::scope) != context.owned_witness_scope
            || request.guard.is_some() || request.idempotency_key().is_some()) {
            Err(Anomaly::incorrect("rtdb.owned/staging-deadline", "A staging deadline requires a fresh placed source attempt"))
        } else if let Some(replay) = replay && let Err(error) = tx.check_replay_marker(replay, receipt_time).await {
            Err(error)
        } else {
            let (receipt, mut outcome) = if request.guard.is_none() {
                match request.idempotency_key() {
                    Some(key) => match request.body_digest() {
                        Ok(digest) => {
                            let id = format!("{}\u{1f}{key}", request.options.user.as_deref().unwrap_or(""));
                            let outcome = tx.receipt_outcome(&id, &digest, receipt_time).await.and_then(|outcome| outcome);
                            (Some((id, digest)), outcome)
                        }
                        Err(error) => (None, Err(error)),
                    },
                    None => (None, Ok(())),
                }
            } else { (None, Ok(())) };
            if has_owned_operations && outcome.is_ok() {
                outcome = tx.check_owned_preconditions(primary, attempt.map(PlacedAttempt::scope), slot.is_some()).await;
            }
            for (index, operation) in primary.iter().enumerate().take_while(|_| outcome.is_ok()) {
                context.check_cancelled()?;
                if let Err(error) = validate_document_operation(operation, request.options.user.as_deref()) {
                    outcome = Err(error);
                    break;
                }
                if let Err(error) = tx.apply(operation).await {
                    let error = match document_table(operation) {
                        Some(table) if !error.context.contains_key("table") => error.with_transit_context("table", &T::table_ref(table))?,
                        _ => error,
                    };
                    let error = if error.code == "xtdb/assert-failed" {
                        match &request.guard {
                            Some(SourceGuard::Application(guard)) => {
                                let guard_time = crate::guard_attribution::guard_time_at(index, guard.after_bootstrap, guard.submitted_at.0);
                                let error = error.with_context(IDEMPOTENCY_GUARD_RESULT, guard_time.map_or(false.into(), Into::into));
                                if let (Some(sample), Some(id)) = (guard_time, request.idempotency_key())
                                    && let Some(original) = tx.historical_guard_original(id, Timestamp(sample)).await?
                                {
                                    crate::guard_attribution::with_historical_original(error, original)
                                } else {
                                    error
                                }
                            }
                            Some(SourceGuard::Replay(_) | SourceGuard::Bounded(_)) => error.with_context(IDEMPOTENCY_GUARD_RESULT, false.into()),
                            None => error,
                        }
                    } else { error };
                    outcome = Err(error.with_context("tx-op-idx", index.into()).with_context("tx-key", tx_key_context(key)?));
                    break;
                }
            }
            let evaluated = has_owned_operations && outcome.is_ok();
            if evaluated {
                outcome = tx.finish_owned_unique().await;
            }
            if outcome.is_ok() {
                outcome = tx.finish_owned_capacity().await;
            }
            if outcome.is_ok() {
                if let Some(replay) = &tx.hosted_replay {
                    outcome = replay.require_constraints_matched().and_then(|()| replay.require_expiries_matched());
                }
            }
            if has_owned_operations && outcome.is_ok() {
                outcome = tx.check_final_constraints(primary).await;
            }
            if has_owned_operations && outcome.is_ok() {
                outcome = tx.check_spatial_readiness(primary).await;
            }
            constraint_failed = evaluated && outcome.as_ref().err().is_some_and(constraint_receipt::may_settle);
            if outcome.is_ok() {
                outcome = tx.write_owned_changes().await;
            }
            if outcome.is_ok() {
                outcome = tx.buffer.require_archived_positions_consumed();
            }
            outcome.and_then(|()| match (replay, receipt) {
                (Some(replay), _) => tx.write_replay_marker(replay),
                (None, Some((id, digest))) => tx.put_receipt(&id, &digest, receipt_time),
                (None, None) => Ok(()),
            })
        };
        let mut settled = false;
        if constraint_failed && !refusal.is_empty() {
            drop(tx);
            tx = OpenTransaction::new(base.clone(), key, &request.options, context)?;
            tx.trace_parent = span.as_ref().map(|span| span.context());
            let _checks = context.memory.reserve(
                (primary.len() + refusal.len()).checked_mul(std::mem::size_of::<Operation>() + 128)
                    .ok_or_else(overflow)?,
            )?;
            let operations = primary.iter().filter(constraint_receipt::shared_check)
                .chain(refusal).cloned().collect::<Vec<_>>();
            outcome = tx.apply_constraint_refusal(&operations, attempt.map(PlacedAttempt::scope)).await;
            settled = outcome.is_ok();
        }
        let result = match outcome {
            Ok(()) => TransactionResult::Committed { tx_key: key },
            Err(error) if error.is_caller_error() => {
                if let Some(observer) = &context.execution_observer { observer.record_error(rtdb_plan::ExecutionError::Transaction); }
                drop(tx);
                tx = OpenTransaction::new(base.clone(), key, &request.options, context)?;
                TransactionResult::Aborted { tx_key: key, error: Some(crate::RetainedAnomaly::new(error, &context.memory)?) }
            }
            Err(error) => return Err(error),
        };
        if let Some(receipt) = bounded_receipt { tx.write_bounded_receipt(receipt, receipt_time)?; }
        if !has_owned_operations { tx.retire_expired_receipts(clock).await?; }
        let input = attempt.map_or(ResolvedInput::Position(position), |attempt| ResolvedInput::Placed {
            attempt, retirement: context.placed_receipt_retirement,
        });
        let selected = if settled { refusal } else { primary };
        let mut resolved = tx.finish_source(result, input, log_time, clock, selected).await?;
        if resolved.result().committed() && let Some((until, journal_before)) = staging_deadline
            && let Err(error) = resolved.prepare_inline_replica(writer.as_ref(), until, journal_before, &context.memory)
        {
            if !error.is_caller_error() { return Err(error); }
            drop(resolved);
            if let Some(observer) = &context.execution_observer { observer.record_error(rtdb_plan::ExecutionError::Transaction); }
            let tx = OpenTransaction::new(base, key, &request.options, context)?;
            let result = TransactionResult::Aborted { tx_key: key, error: Some(crate::RetainedAnomaly::new(error, &context.memory)?) };
            resolved = tx.finish_source(result, input, log_time, clock, &[]).await?;
        }
        context.check_cancelled()?;
        self.stage_resolved(resolved)
    }

    pub async fn resolve_external(&mut self, durable: Snapshot, now: Timestamp, system_time: Option<Timestamp>, token: Option<Vec<u8>>, writer: &mut dyn TransactionWriter, context: &ResolveContext) -> Result<Arc<ResolvedTx>> {
        context.check_cancelled()?;
        let durable = rtdb_query::retain_catalog_snapshot(durable, &context.storage).await?;
        let base = self.resolving_snapshot(durable)?;
        let id = self.resolved_head.map_or(-1, |key| key.tx_id.0).checked_add(1).ok_or_else(overflow)?;
        let key = TransactionKey { tx_id: TxId(id), system_time: self.smooth(system_time.map_or(now, |time| Timestamp(time.0.min(now.0))))? };
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
        let clock = tx.observe_retention_clock(now).await?;
        tx.retire_expired_receipts(clock).await?;
        tx.write_result(&result, metadata.as_ref(), now, clock.time)?;
        context.check_cancelled()?;
        self.stage(tx.buffer.stage()?, result, ResolvedInput::Position(self.source_position), token, None, context)
    }

    pub async fn resolve_db_op(&mut self, durable: Snapshot, position: SourcePosition, log_time: Timestamp, operation: DbOp, verdict: Result<()>, context: &ResolveContext) -> Result<Arc<ResolvedTx>> {
        let durable = rtdb_query::retain_catalog_snapshot(durable, &context.storage).await?;
        let base = self.resolving_snapshot(durable)?;
        let key = TransactionKey { tx_id: TxId(position.0), system_time: self.smooth(log_time)? };
        let (result, operation) = match verdict {
            Ok(()) => (TransactionResult::Committed { tx_key: key }, Some(operation)),
            Err(error) if error.is_caller_error() => (TransactionResult::Aborted { tx_key: key, error: Some(crate::RetainedAnomaly::new(error, &context.memory)?) }, None),
            Err(error) => return Err(error),
        };
        let options = SubmitOptions { default_tz: context.default_tz.clone(), ..SubmitOptions::default() };
        let mut tx = OpenTransaction::new(base, key, &options, context)?;
        let clock = tx.observe_retention_clock(log_time).await?;
        let receipt_time = clock.time;
        tx.retire_expired_receipts(clock).await?;
        tx.write_result(&result, None, log_time, receipt_time)?;
        context.check_cancelled()?;
        self.stage(tx.buffer.stage()?, result, ResolvedInput::Position(position), None, operation, context)
    }

    fn smooth(&self, candidate: Timestamp) -> Result<Timestamp> {
        crate::system_time::automatic_time(self.previous_system_time(), candidate.0)
            .map(Timestamp)
            .ok_or_else(|| Anomaly::incorrect("xtdb/time-overflow", "Timestamp has no following chronon"))
    }

    fn previous_system_time(&self) -> Option<i64> { self.resolved_head.map(|key| key.system_time.0) }

    fn stage(&mut self, writes: StagedWrite, result: TransactionResult, input: ResolvedInput, token: Option<Vec<u8>>, db_op: Option<DbOp>, context: &ResolveContext) -> Result<Arc<ResolvedTx>> {
        self.stage_resolved(ResolvedTx::new(writes, result, input, token, db_op, context)?)
    }

    fn stage_resolved(&mut self, resolved: ResolvedTx) -> Result<Arc<ResolvedTx>> {
        let position = resolved.source_position();
        if position < self.source_position { return Err(Anomaly::fault("rtdb.tx/source-watermark", "Resolve-side source watermark moved backwards")); }
        let key = resolved.result.tx_key();
        if resolved.placed_attempt().is_some() && key.tx_id.0 != position.0 {
            return Err(Anomaly::fault("rtdb.tx/placed-attempt-position", "A placed attempt must match its transaction result"));
        }
        if self.resolved_head.is_some_and(|head| head.tx_id >= key.tx_id) {
            return Err(Anomaly::fault("rtdb.tx/transaction-order", "Resolved transaction position does not advance its predecessor"));
        }
        if resolved.writes.tx_key() != key || !crate::system_time::advances(self.previous_system_time(), key.system_time.0) {
            return Err(Anomaly::fault("rtdb.tx/system-time", "Resolved transaction does not advance its predecessor's system time"));
        }
        let next_token_charge = resolved.external_token.as_ref().map(|token| self.memory.reserve(token.capacity().checked_add(512).ok_or_else(overflow)?)).transpose()?;
        if self.queue.len() == self.queue.capacity() {
            let capacity = self.queue.capacity().max(4).checked_mul(2).ok_or_else(overflow)?;
            let charge = self.memory.reserve(capacity.checked_mul(std::mem::size_of::<Arc<ResolvedTx>>()).and_then(|bytes| bytes.checked_add(128)).ok_or_else(overflow)?)?;
            self.queue.reserve_exact(capacity - self.queue.len());
            self.queue_reservation = charge;
        }
        let resolved = Arc::new(resolved);
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

fn independent_world_observations(operations: &[Operation], publishing: &BTreeSet<Iid>) -> BTreeSet<Iid> {
    operations.iter().filter_map(|operation| match operation {
        Operation::Public(TxOp::OwnedCheckWorldPurge { relation, .. }) => relation.world(),
        Operation::Public(TxOp::OwnedCheckWorldScope { clock: Some(clock), .. }) => Some(clock.world),
        _ => None,
    }).filter(|world| !publishing.contains(world)).collect()
}

#[cfg(test)]
#[test]
fn batch_observers_exclude_the_publishing_world_and_include_clock_only_worlds() -> Result<()> {
    let writer = Iid([1; 16]);
    let clock_only = Iid([2; 16]);
    let scope = rtdb_types::PlacedAttemptScope::new("held-batch-observations", 1)?;
    let guarded = rtdb_types::OwnedRelation::new("facts")?.in_world(writer)?;
    let operations = [
        Operation::Public(TxOp::OwnedCheckWorldPurge {
            relation: guarded, pin: writer, frontier: 0, local_frontier: None,
        }),
        Operation::Public(TxOp::OwnedCheckWorldScope {
            scope,
            clock: Some(rtdb_types::WorldClockLimit { world: clock_only, pin: clock_only, before: Timestamp(7) }),
        }),
    ];
    assert_eq!(independent_world_observations(&operations, &BTreeSet::from([writer])),
        BTreeSet::from([clock_only]));
    Ok(())
}

struct FixedRangeSide {
    id: EntityId,
    before: Value,
    after: Value,
}

#[derive(Default)]
struct PendingOwnedChanges {
    bytes: usize,
    erased: bool,
    touches: Vec<Value>,
    fixed_sides: Option<Vec<FixedRangeSide>>,
    fixed_bytes: usize,
    reservations: Vec<Arc<MemoryReservation>>,
}

pub struct OpenTransaction<'a> {
    buffer: TransactionBuffer, options: &'a SubmitOptions, context: &'a ResolveContext, trace_parent: Option<rtdb_plan::SpanContext>,
    unique_changes: OwnedUniqueChanges,
    capacity_changes: OwnedCapacityChanges,
    owned_worlds: BTreeMap<Iid, (rtdb_types::OwnedWorld, Arc<MemoryReservation>)>,
    timed_world_overlays: BTreeMap<Iid, Timestamp>,
    owned_changes: BTreeMap<String, PendingOwnedChanges>,
    bounded_period_writes: BTreeMap<(rtdb_types::OwnedRelation, rtdb_types::EntityId), Arc<MemoryReservation>>,
    hosted_replay: Option<crate::hosted_control::HostedReplay>,
}
impl<'a> OpenTransaction<'a> {
    fn new(base: Snapshot, key: TransactionKey, options: &'a SubmitOptions, context: &'a ResolveContext) -> Result<Self> {
        Ok(Self { buffer: TransactionBuffer::new(base, key, Arc::clone(&context.memory))?, options, context, trace_parent: None, unique_changes: OwnedUniqueChanges::default(), capacity_changes: OwnedCapacityChanges::default(), owned_worlds: BTreeMap::new(), timed_world_overlays: BTreeMap::new(), owned_changes: BTreeMap::new(), bounded_period_writes: BTreeMap::new(), hosted_replay: None })
    }
    pub fn tx_key(&self) -> TransactionKey { self.buffer.tx_key() }

    async fn finish_source(
        mut self, result: TransactionResult, input: ResolvedInput, log_time: Timestamp,
        clock: retention::RetentionClock, selected: &[Operation],
    ) -> Result<ResolvedTx> {
        let options = self.options;
        self.write_result(&result, options.user_metadata.as_ref(), log_time, clock.time)?;
        if let ResolvedInput::Placed { attempt, .. } = input {
            let controls = selected.iter().filter_map(|operation| match operation {
                Operation::Public(TxOp::HostedControl(control)) if result.committed() && !control.is_check() => Some(control.name()),
                _ => None,
            }).collect();
            let worlds = if result.committed() { crate::replica::worlds::source_inventory(selected)? } else { Vec::new() };
            self.retire_placed_outcomes(clock).await?;
            let archive_replay = result.committed() && self.hosted_replay.is_some();
            self.write_attempt_outcome(attempt, log_time, controls, worlds, archive_replay)?;
        }
        ResolvedTx::new(self.buffer.stage()?, result, input, None, None, self.context)
    }

    /// Constraint checks interpret mutations at their authored valid time.
    /// Held replay retains that time while ordering new system revisions.
    fn owned_constraint_present(&self) -> Timestamp {
        self.hosted_replay.as_ref().map_or(self.tx_key().system_time, |replay| replay.source_time())
    }

    pub fn snapshot(&self) -> Result<Snapshot> { self.buffer.snapshot() }
    pub fn append(&mut self, table: TableRef, event: StoredEvent) -> Result<()> {
        self.context.check_cancelled()?;
        if is_replay_table(&table) || table == receipts_table() || is_bounded_receipts_table(&table) || TableRef::owned_schema(&table.schema) {
            check_table(&table)?;
        }
        self.buffer.append(table, event)
    }
    /// Stages a source's Arrow documents in their input order under this transaction's system time.
    pub fn append_arrow_puts(
        &mut self, table: &TableRef, documents: arrow_array::ArrayRef, headers: &[EventHeader],
    ) -> Result<()> {
        self.context.check_cancelled()?;
        if is_replay_table(table) || *table == receipts_table() || is_bounded_receipts_table(table) || TableRef::owned_schema(&table.schema) { check_table(table)?; }
        let batch = rtdb_format::data::owned_put_batch(documents, headers, None, &self.context.memory)?;
        self.buffer.append_batch(table, batch)
    }
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
        self.execute_sql_typed(sql, arguments.map(|values| BoundArgumentRow::Values { values, types: None }), &mut None).await
    }

    /// `planned` carries one operation's mutation from each argument row to the next, which reuses it while the
    /// transaction's catalog answers the planning's lookups the same.
    async fn execute_sql_typed(&mut self, sql: &str, arguments: Option<BoundArgumentRow<'_>>, planned: &mut Option<PlannedMutation>) -> Result<()> {
        self.context.check_cancelled()?;
        // A statement whose plan reads no table can't see the transaction's own writes, so it skips extending the snapshot with them.
        let snapshot = if planned.as_ref().is_some_and(|planned| !planned.reads) { self.buffer.base_snapshot() } else { self.snapshot()? };
        let context = self.context.query_context(snapshot, self.tx_key().system_time, &self.options.default_tz, self.trace_parent)?;
        let parameter_types = match arguments {
            Some(BoundArgumentRow::Values { types: Some(types), .. } | BoundArgumentRow::Arrow { types, .. }) =>
                types.iter().map(rtdb_columnar::execution_type).enumerate().collect(),
            Some(BoundArgumentRow::Values { values, .. }) =>
                values.iter().enumerate().map(|(index, value)| (index, rtdb_columnar::execution_type(&value.logical_type()))).collect(),
            None => Default::default(),
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

    /// Whether a live receipt names this retry identity: the same body is a duplicate of the original, a different one a conflict.
    async fn receipt_outcome(&self, id: &str, digest: &str, log_time: Timestamp) -> Result<Result<()>> {
        if !self.snapshot()?.contains_table(&receipts_table()) { return Ok(Ok(())); }
        let bucket = receipt_bucket(log_time);
        let mut query = self.open_query(
            "SELECT digest, tx_id, tx_system_time FROM xt.idempotency_receipts FOR ALL VALID_TIME WHERE _id IN (?, ?) AND (_valid_to IS NULL OR _valid_to > ?) ORDER BY tx_system_time DESC LIMIT 1",
            vec![receipt_id(id, bucket), receipt_id(id, bucket - 1), time_value(log_time)],
        ).await?;
        while let Some(batch) = query.next_batch().await? {
            let batch = batch.batch();
            if batch.num_rows() == 0 { continue; }
            let cell = |index: usize| rtdb_columnar::read_value(batch.schema().field(index), batch.column(index).as_ref(), 0);
            let (Value::Utf8(stored), Value::I64(tx_id), Value::Timestamp { value: system_time, .. }) = (cell(0)?, cell(1)?, cell(2)?) else {
                return Err(Anomaly::fault("rtdb.tx/idempotency-receipt", "A retry receipt has no digest or original transaction"));
            };
            let original = tx_key_context(TransactionKey { tx_id: TxId(tx_id), system_time: Timestamp(system_time) })?;
            let error = if stored == digest {
                Anomaly::incorrect("rtdb/idempotency-duplicate", "A transaction with this idempotency key already committed")
            } else {
                Anomaly::new(rtdb_types::AnomalyCategory::Conflict, "rtdb/idempotency-conflict", "This idempotency key already committed a different transaction")
            };
            return Ok(Err(error.with_context("original", original)
                .with_context("original-tx-id", tx_id.into()).with_context("original-system-time-micros", system_time.into())));
        }
        Ok(Ok(()))
    }

    fn put_receipt(&mut self, id: &str, digest: &str, log_time: Timestamp) -> Result<()> {
        let key = self.tx_key();
        let document = StructFields::from([
            ("_id".into(), receipt_id(id, receipt_bucket(log_time))), ("digest".into(), Value::Utf8(digest.into())),
            ("tx_id".into(), Value::I64(key.tx_id.0)), ("tx_system_time".into(), time_value(key.system_time)),
        ]);
        let expires = Timestamp(log_time.0.saturating_add(RECEIPT_WINDOW_MICROS));
        self.buffer.put(receipts_table(), document, interval(log_time, expires)?)
    }

    fn write_attempt_outcome(
        &mut self, attempt: PlacedAttempt, log_time: Timestamp,
        controls: std::collections::BTreeSet<&str>, worlds: Vec<Value>, archive_replay: bool,
    ) -> Result<()> {
        let key = self.tx_key();
        let mut document = StructFields::from([
            ("_id".into(), Value::Utf8(attempt.outcome_key())),
            ("tx_id".into(), Value::I64(key.tx_id.0)),
            ("tx_system_time".into(), time_value(key.system_time)),
        ]);
        if !controls.is_empty() {
            document.insert(crate::hosted_control::CONTROL_TABLES.into(),
                Value::list(controls.into_iter().map(|name| Value::Utf8(name.into())).collect()));
        }
        if !worlds.is_empty() {
            document.insert(crate::replica::worlds::WORLD_EFFECTS.into(), Value::list(worlds));
        }
        if archive_replay {
            document.insert(crate::hosted_control::RESOLVED_ARCHIVE_REPLAY.into(), Value::Bool(true));
        }
        let expires = Timestamp(log_time.0.saturating_add(RECEIPT_WINDOW_MICROS));
        self.buffer.put(TableRef::new("xt", PLACED_ATTEMPT_OUTCOME_TABLE), document, interval(log_time, expires)?)
    }

    async fn index_exists(&self, catalog: &str, name: &str) -> Result<bool> {
        if !self.snapshot()?.contains_table(&TableRef::new("xt", catalog)) { return Ok(false); }
        let sql = format!("SELECT _id FROM xt.{catalog} WHERE _id = ?");
        let mut query = self.open_query(&sql, vec![Value::Utf8(name.into())]).await?;
        Ok(query.next_batch().await?.is_some())
    }

    async fn execute_mutation(&mut self, mutation: rtdb_sql::Mutation, context: Arc<QueryContext>, arguments: Option<BoundArgumentRow<'_>>, syntax: Option<Arc<MemoryReservation>>) -> Result<()> {
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
            Mutation::CreateTable { table, columns } => {
                let table = table.table;
                self.declare_table(table.clone(), columns.iter().map(|(name, _)| name.clone()))?;
                return self.buffer.declare_document_types(&table, columns.iter().map(|(name, ty)| (name, ty)));
            },
            Mutation::CreateSearchIndex { name, table, columns, history, if_not_exists } => {
                if table.database != self.context.database { return Err(Anomaly::incorrect("xtdb/table-not-found", "A search index cannot cover another database's table")); }
                if TableRef::owned_schema(&table.table.schema) { check_table(&table.table)?; }
                check_search_index_name(&name)?;
                if if_not_exists && self.index_exists("search_indexes", &name).await? { return Ok(()); }
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
                if self.index_exists("ordered_indexes", &name).await? {
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
                if !self.index_exists("ordered_indexes", &name).await? {
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
                return self.buffer.append(TableRef::new("xt", "role_membership"), StoredEvent { origin: None, commit_position: 0, entity_key: None, iid, system_from: self.tx_key().system_time, valid_time: interval(self.tx_key().system_time, Timestamp::MAX)?, operation, recency: None });
            }
        };
        let table = table.ok_or_else(|| Anomaly::fault("rtdb.tx/mutation-table", "Mutation has no target"))?;
        if table.database != self.context.database { return Err(Anomaly::incorrect("xtdb/table-not-found", "Mutation cannot write to another database")); }
        if matches!(kind, MutationKind::Put)
            && table.table.schema == "xt" && table.table.table == HOSTED_CAPACITY_DECLARATIONS
        {
            let writer = self.hosted_capacity_writer().await?;
            let cursor = execute(input, context, arguments, syntax).await?;
            return self.stage_hosted_capacity_declarations(cursor, writer).await;
        }
        check_table(&table.table)?;
        let mut cursor = execute(input, context, arguments, syntax).await?;
        let system_time = self.tx_key().system_time;
        while let Some(batch) = cursor.next_batch().await? {
            let batch = batch.batch();
            match kind {
                MutationKind::Put => self.stage_mutation_puts(&table.table, batch, system_time)?,
                _ => mutation_events(batch, kind, system_time, |event| self.buffer.append_to(&table.table, event).map(drop))?,
            }
        }
        Ok(())
    }

    fn stage_mutation_puts(&mut self, table: &TableRef, batch: &RecordBatch, system_time: Timestamp) -> Result<()> {
        if batch.num_rows() == 0 { return Ok(()); }
        let memory = &self.context.memory;
        match mutation_put_batch(batch, system_time, memory) {
            Ok(Some(put)) => return self.buffer.append_batch(table, put),
            Ok(None) => (),
            Err(error) if error.code == "rtdb/memory-budget-exceeded" => (),
            Err(error) => return Err(error),
        }

        let iids = mutation_column(batch, "_iid")?;
        let from = mutation_column(batch, "_valid_from")?;
        let to = mutation_column(batch, "_valid_to")?;
        let documents = batch.column_by_name("doc")
            .ok_or_else(|| Anomaly::fault("rtdb.tx/mutation-document", "Put mutation has no document column"))?;
        for row in 0..batch.num_rows() {
            let header = EventHeader {
                commit_position: 0,
                iid: iids.iid(row)?, system_from: system_time,
                valid_time: interval(from.time(row, system_time)?, to.time(row, Timestamp::MAX)?)?, op: EventOp::Put,
            };
            let (document, index) = mutation_document(documents.as_ref(), row)
                .ok_or_else(|| Anomaly::fault("rtdb.tx/mutation-document", "Put mutation has no document"))?;
            let content = document.slice(index, 1);
            let batch = rtdb_format::data::owned_put_batch(Arc::new(content), &[header], None, memory)?;
            self.buffer.append_batch(table, batch)?;
        }
        Ok(())
    }

    async fn apply(&mut self, operation: &Operation) -> Result<()> {
        let span = match operation {
            Operation::Public(TxOp::Sql { sql, .. } | TxOp::SqlBytes { sql, .. }) => self.context.transaction_span(self.trace_parent, "xtdb.transaction.sql", &[("query.text", rtdb_plan::TraceValue::Text(sql))]),
            Operation::Public(TxOp::PatchDocs { table, .. }) | Operation::PatchIids { table, .. } | Operation::PatchArrowIids { table, .. } => self.context.transaction_span(self.trace_parent, "xtdb.transaction.patch-docs", &[("db", rtdb_plan::TraceValue::Text(&self.context.database)), ("schema", rtdb_plan::TraceValue::Text(&table.schema)), ("table", rtdb_plan::TraceValue::Text(&table.table))]),
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
                    for row in arguments.indexed_bound_rows() {
                        let (index, row) = row?;
                        self.execute_sql_typed(sql, Some(row), &mut planned)
                            .await.map_err(|error| error.with_context("arg-idx", index.into()))?;
                    }
                    Ok(())
                }
                None => self.execute_sql(sql, None).await,
            },
            Operation::Public(TxOp::SqlBytes { sql, arg_bytes }) => {
                if arg_bytes.as_slice().is_empty() { return self.execute_sql(sql, None).await; }
                let arguments = decode_arguments(arg_bytes.as_slice(), &self.context.memory)?;
                let mut planned = None;
                for row in arguments.indexed_bound_rows() {
                    let (index, row) = row?;
                    self.execute_sql_typed(sql, Some(row), &mut planned)
                        .await.map_err(|error| error.with_context("arg-idx", index.into()))?;
                }
                Ok(())
            }
            Operation::Public(TxOp::PutArrowDocs { table, documents }) => self.put_arrow_documents(table, documents, None, None, None, false).await,
            Operation::PutReplayArrow { table, documents, iids, valid_from, valid_to } => self.put_arrow_documents(table, documents, iids.as_deref(), *valid_from, *valid_to, false).await,
            Operation::Public(TxOp::OwnedDeclareRelation { relation, schema }) =>
                self.declare_owned_relation(relation, schema).await,
            Operation::Public(TxOp::OwnedDeclareFixedRange { relation, rule }) => self.declare_fixed_range(relation, rule).await,
            Operation::Public(TxOp::OwnedDeclareCapacity { relation, column, prefix_bytes, cap, when }) =>
                self.declare_owned_capacity(relation, column, *prefix_bytes, *cap, when).await,
            Operation::Public(TxOp::OwnedDeclareUniquePath { relation, name, column }) =>
                self.declare_owned_unique_path(relation, name, column).await,
            Operation::Public(TxOp::OwnedDeclarePath { relation, name, columns }) =>
                self.declare_owned_path(relation, name, columns, OwnedPathKind::Ordered).await,
            Operation::Public(TxOp::OwnedDropSpatialPath { relation, name }) =>
                self.drop_owned_spatial_path(relation, name).await,
            Operation::Public(TxOp::OwnedRetireRelation { relation, retain_micros }) =>
                self.retire_owned_relation(relation, *retain_micros).await,
            Operation::Public(TxOp::OwnedDeclareSpatialPath { relation, name, columns }) =>
                self.declare_owned_path(relation, name, columns, OwnedPathKind::Spatial(rtdb_types::SpatialGeometryEncoding::TaggedWkb)).await,
            Operation::Public(TxOp::OwnedDeclareEncodedSpatialPath { relation, name, columns, encoding }) =>
                self.declare_owned_path(relation, name, columns, OwnedPathKind::Spatial(*encoding)).await,
            Operation::Public(TxOp::OwnedDeclareSearchPath { relation, name, column }) =>
                self.declare_owned_path(relation, name, std::slice::from_ref(column), OwnedPathKind::Search).await,
            Operation::Public(TxOp::HostedConstraintRefusal) => {
                Err(Anomaly::incorrect("rtdb.hosted/constraint-receipt", "Unexpected refusal boundary"))
            }
            Operation::Public(TxOp::HostedControl(control)) => {
                if self.hosted_replay.as_ref().is_some_and(|replay| replay.is_transient_world_control(control)) {
                    return Ok(());
                }
                if let Some(documents) = control.arrow_replacement() {
                    return self.put_arrow_documents(&control.table(), documents, Some(&[control.id().iid()]),
                        Some(Timestamp(0)), Some(Timestamp::MAX), false).await;
                }
                if let Some(replacement) = control.replacement() {
                    let valid = interval(Timestamp(0), Timestamp::MAX)?;
                    let mut document = replacement.clone();
                    if control.name() == rtdb_types::HOSTED_ROOT_RECEIPTS_TABLE {
                        document.insert(rtdb_types::HOSTED_RECEIPT_TX_ID.into(), Value::I64(self.tx_key().tx_id.0));
                    }
                    self.buffer.put(control.table(), document, valid)?;
                }
                Ok(())
            }
            Operation::Public(TxOp::OwnedForkWorld { record, previous, sibling, attempt, fingerprint, root, .. }) => {
                self.reserve_owned_attempt(attempt, *fingerprint).await?;
                let documents = crate::Documents::new(vec![record.clone()], &self.context.memory)?;
                let relation = rtdb_types::OwnedRelation::world_catalog();
                if matches!(root, crate::OwnedForkRoot::WritesAdvance | crate::OwnedForkRoot::WritesRenewal | crate::OwnedForkRoot::ProposalHold | crate::OwnedForkRoot::ProposalShorten) {
                    // The ordered preconditions compared the exact original
                    // registration and its lease on the base snapshot.
                    let owner = self.options.user.as_deref().ok_or_else(no_owned_principal)?;
                    self.put_documents(&relation.table(owner), &documents, None, Some(Timestamp::MIN), Some(Timestamp::MAX),
                        DocumentWrite::Owned { relation: &relation, owner, create_only: false }).await?;
                } else {
                    self.write_owned(&relation, &documents, None, None, None, previous.is_none() || sibling.is_some()).await?;
                }
                if matches!(*root, crate::OwnedForkRoot::Writes | crate::OwnedForkRoot::WritesAdvance | crate::OwnedForkRoot::WritesRenewal | crate::OwnedForkRoot::WritesSibling | crate::OwnedForkRoot::ProposalWrites | crate::OwnedForkRoot::ProposalHold | crate::OwnedForkRoot::ProposalShorten) {
                    self.publish_owned_root_head().await?;
                }
                Ok(())
            }
            Operation::Public(TxOp::OwnedPublishRoot { attempt, fingerprint }) => {
                self.reserve_owned_attempt(attempt, *fingerprint).await?;
                self.publish_owned_root_head().await
            }
            Operation::Public(TxOp::OwnedPublishWorld { world, parent_pin, attempt, fingerprint, expected, advance_to, drop_world, settlement_pin, land_root, delete_root }) => {
                self.publish_owned_world(*world, *parent_pin, attempt, *fingerprint, *expected, *advance_to, *drop_world, *settlement_pin).await?;
                if delete_root.is_some() || land_root.is_some() { self.publish_owned_root_head().await?; }
                Ok(())
            },
            Operation::Public(TxOp::OwnedRequireSpatialPath { .. }) => Ok(()),
            Operation::Public(TxOp::OwnedDeadline { .. } | TxOp::OwnedClockSlot { .. } | TxOp::OwnedCheckWorldScope { .. } | TxOp::OwnedCheckWorldPurge { .. } | TxOp::OwnedCheckRange { .. } | TxOp::OwnedCheckCapacity { .. } | TxOp::OwnedCheck { .. }) => Ok(()),
            Operation::Public(TxOp::OwnedErase { relation, id }) =>
                self.erase_owned(relation, id, rtdb_types::OwnedChangeKind::Erase).await,
            Operation::Public(TxOp::OwnedSweepExpired { relation, id, created, expiry, .. }) =>
                self.sweep_owned_expiry(relation, id, *created, *expiry).await,
            Operation::Public(TxOp::OwnedDelete { relation, id, valid_from, valid_to }) =>
                self.delete_owned(relation, id, *valid_from, *valid_to).await,
            Operation::Public(TxOp::OwnedArrowDocs { relation, documents, create_only, valid_from, valid_to }) =>
                self.write_owned_arrow(relation, documents, None, *valid_from, *valid_to, *create_only).await,
            Operation::OwnedArrowIids { relation, documents, iids, presence, create_only, valid_from, valid_to } =>
                self.write_owned_arrow(relation, documents, Some((iids, presence)), *valid_from, *valid_to, *create_only).await,
            Operation::Public(TxOp::OwnedInsertDocs { relation, documents, valid_from, valid_to }) =>
                self.write_owned(relation, documents, None, *valid_from, *valid_to, true).await,
            Operation::Public(TxOp::OwnedRelativePeriod { .. }) => Err(Anomaly::incorrect(
                "rtdb.owned/validity-policy", "The Root period must be placed before staging",
            )),
            Operation::Public(TxOp::OwnedReplaceDocs { relation, documents, valid_from, valid_to }) =>
                self.write_owned(relation, documents, None, *valid_from, *valid_to, false).await,
            Operation::OwnedWriteIids { create_only, relation, documents, iids, valid_from, valid_to } =>
                self.write_owned(relation, documents, Some(iids), *valid_from, *valid_to, *create_only).await,
            Operation::Public(TxOp::InsertDocs { table, documents, valid_from, valid_to }) => self.put_documents(table, documents, None, *valid_from, *valid_to, DocumentWrite::Insert).await,
            Operation::InsertIids { table, documents, iids, valid_from, valid_to } => self.put_documents(table, documents, Some(iids), *valid_from, *valid_to, DocumentWrite::Insert).await,
            Operation::InsertArrowIids { table, documents, iids, valid_from, valid_to } =>
                self.put_arrow_documents(table, documents, Some(iids), *valid_from, *valid_to, true).await,
            Operation::Public(TxOp::PutDocs { table, documents, valid_from, valid_to }) => self.put_documents(table, documents, None, *valid_from, *valid_to, DocumentWrite::Put).await,
            Operation::PutIids { table, documents, iids, valid_from, valid_to } => self.put_documents(table, documents, Some(iids), *valid_from, *valid_to, DocumentWrite::Put).await,
            Operation::Public(TxOp::DeleteDocs { table, ids, valid_from, valid_to }) => {
                check_table(table)?;
                let interval = interval(valid_from.unwrap_or(self.tx_key().system_time), valid_to.unwrap_or(Timestamp::MAX))?;
                for id in ids.ids() { self.buffer.delete(table.clone(), id, interval)?; }
                Ok(())
            }
            Operation::Public(TxOp::EraseDocs { table, ids }) => { check_table(table)?; for id in ids.ids() { self.buffer.erase(table.clone(), id)?; } Ok(()) }
            Operation::Public(TxOp::PatchDocs { table, documents, valid_from, valid_to }) => self.patch_documents(table, documents, None, *valid_from, *valid_to).await,
            Operation::PatchIids { table, documents, iids, valid_from, valid_to } => self.patch_documents(table, documents, Some(iids), *valid_from, *valid_to).await,
            Operation::PatchArrowIids { table, documents, iids, valid_from, valid_to } => self.patch_arrow_documents(table, documents, Some(iids), *valid_from, *valid_to).await,
            Operation::Removed(name) => Err(Anomaly::unsupported(if name == "xtql" { "xtdb/xtql-dml-removed" } else { "xtdb/tx-fns-removed" }, if name == "xtql" { "XTQL DML is no longer supported, as of 2.0.0-beta7. Please use SQL DML statements instead - see the release notes for more information." } else { "tx-fns are no longer supported, as of 2.0.0-beta7. Please use ASSERTs and SQL DML statements instead - see the release notes for more information." })),
        }
    }
    async fn patch_documents(&mut self, table: &TableRef, documents: &crate::Documents, iids: Option<&[Iid]>, from: Option<Timestamp>, to: Option<Timestamp>) -> Result<()> {
        let documents = crate::source::owned_row_documents(documents, &self.context.memory)?;
        self.patch_arrow_documents(table, &documents, iids, from, to).await
    }

    async fn patch_arrow_documents(&mut self, table: &TableRef, documents: &ArrowDocuments, iids: Option<&[Iid]>, from: Option<Timestamp>, to: Option<Timestamp>) -> Result<()> {
        use rtdb_plan::{Binder, Expr, NamedExpr, Relation, RelationNode, Schema};
        let valid = interval(from.unwrap_or(self.tx_key().system_time), to.unwrap_or(Timestamp::MAX))?;
        if documents.array().is_empty() { return Ok(()); }
        let derived;
        let _iids;
        let iids = if let Some(iids) = iids {
            iids
        } else {
            let memory = &self.context.memory;
            _iids = memory.reserve(documents.array().len().checked_mul(std::mem::size_of::<Iid>()).ok_or_else(overflow)?)?;
            let reader = documents.id_reader()?;
            derived = (0..documents.array().len()).map(|row| documents.iid(row, reader.as_ref(), memory)).collect::<Result<Vec<_>>>()?;
            &derived
        };
        let batch = documents.patch_parameter(iids, &self.context.memory)?;
        let ty = rtdb_columnar::from_field(batch.batch().schema_ref().field(0))?;
        let LogicalType::List { element, .. } = &ty else {
            return Err(Anomaly::fault("rtdb.tx/patch-parameters", "Patch argument has no documents"));
        };
        let mut binder = Binder::default();
        let element = binder.field("patch_doc", None, element.as_ref().clone())?;
        let expr = Expr::Parameter { index: 0, data_type: ty.clone() };
        let rows = Arc::new(Relation::new(Schema::new(vec![element.clone()])?, RelationNode::ColumnTable { expr, element: element.clone() })?);
        let expressions = ["_iid", "doc"].into_iter().map(|name| {
            let data_type = rtdb_plan::field_type(&element.data_type, name);
            Ok(NamedExpr {
                field: binder.field(name, None, data_type.clone())?,
                expr: Expr::Field { expr: Box::new(element.expression()), name: name.into(), data_type },
            })
        }).collect::<Result<Vec<_>>>()?;
        let input = Arc::new(rows.project(expressions)?);
        self.execute_patch(table, input, valid, Some(BoundArgumentRow::Arrow { batch: &batch, row: 0, types: &[ty] })).await
    }

    async fn execute_patch(&mut self, table: &TableRef, input: Arc<rtdb_plan::Relation>, valid: Interval, arguments: Option<BoundArgumentRow<'_>>) -> Result<()> {
        use rtdb_plan::{Expr, QualifiedTable, TemporalFilter};
        let context = self.context.query_context(self.snapshot()?, self.tx_key().system_time, &self.options.default_tz, self.trace_parent)?;
        let temporal = TemporalFilter::FromTo {
            from: Expr::Literal(time_value(valid.lower())),
            to: Expr::Literal(time_value(valid.upper())),
        };
        let table = QualifiedTable { database: self.context.database.clone(), table: table.clone() };
        let mutation = rtdb_sql::prepare_patch(table, input, temporal, &context.catalog)?;
        self.execute_mutation(mutation, context, arguments, None).await
    }
    async fn declare_owned_relation(&mut self, relation: &rtdb_types::OwnedRelation, schema: &rtdb_types::OwnedSchema) -> Result<()> {
        let owner = self.options.user.as_deref().filter(|owner| !owner.is_empty()).ok_or_else(no_owned_principal)?;
        if relation.world().is_some() || relation.is_world_catalog() { return Err(rtdb_types::OwnedWorld::unavailable()); }
        let conflict = || Anomaly::new(AnomalyCategory::Conflict, "rtdb.owned/relation-exists",
            "The owned relation already has a different declaration or inferred data");
        if let Some((existing, _)) = self.read_owned_schema(owner, relation).await? {
            return if &existing == schema { Ok(()) } else { Err(conflict()) };
        }
        let snapshot = self.snapshot()?;
        if snapshot.contains_table(&relation.table(owner)) {
            return Err(conflict());
        }
        if let Some(duration) = schema.lifetime_micros() {
            if !relation.is_root_data() || snapshot.contains_table(&relation.unique_table(owner))
                || snapshot.contains_table(&relation.capacity_table(owner))
            {
                return Err(lifetime_uncovered());
            }
            let floor = rtdb_types::OwnedLifetimeFloor::at(duration, self.tx_key().system_time)?;
            self.buffer.put(relation.lifetime_table(owner), floor.record(relation), Interval::ALL)?;
        }
        self.check_capacity_schema(snapshot.clone(), owner, relation, schema).await?;
        self.check_unique_schema(snapshot, owner, relation, schema).await?;
        let _encoded = self.context.memory.reserve(schema.retained_bytes() * 8 + 4096)?;
        let document = StructFields::from([
            ("_id".into(), Value::Utf8(relation.name().into())),
            ("schema".into(), Value::Binary(schema.to_json()?)),
        ]);
        self.buffer.put(rtdb_types::OwnedRelation::relation_catalog(owner), document, Interval::ALL)
    }

    async fn read_owned_schema(&self, owner: &str, relation: &rtdb_types::OwnedRelation) -> Result<Option<(rtdb_types::OwnedSchema, Arc<MemoryReservation>)>> {
        let snapshot = self.snapshot()?;
        if !snapshot.contains_table(&rtdb_types::OwnedRelation::relation_catalog(owner)) {
            return Ok(None);
        }
        let context = self.context.query_context(snapshot, self.tx_key().system_time, &self.options.default_tz, self.trace_parent)?
            .with_accelerators(Arc::from([]));
        rtdb_query::read_owned_relation_schema(context, owner, relation).await
    }

    async fn drop_owned_spatial_path(
        &mut self, relation: &rtdb_types::OwnedRelation, path: &rtdb_types::OwnedRelation,
    ) -> Result<()> {
        let owner = self.options.user.as_deref().filter(|owner| !owner.is_empty()).ok_or_else(no_owned_principal)?;
        if relation.world().is_some() || relation.is_world_catalog() { return Err(rtdb_types::OwnedWorld::unavailable()); }
        let catalog = rtdb_types::OwnedRelation::spatial_catalog(owner);
        let target = relation.table(owner);
        let snapshot = self.snapshot()?;
        let context = self.context.query_context(snapshot, self.tx_key().system_time, &self.options.default_tz, self.trace_parent)?;
        let Some(mut cursor) = rtdb_query::open_owned_path_catalog(context, &catalog, Some(path.name())).await? else {
            return Ok(());
        };
        let mut present = false;
        while let Some(batch) = cursor.next_batch().await? {
            let batch = batch.batch();
            for row in 0..batch.num_rows() {
                let text = |field| ArrowCell::named(batch, field, row).and_then(ArrowCell::utf8);
                if present
                    || text("_id") != Some(path.name())
                    || text("schema_name") != Some(target.schema.as_str())
                    || text("table_name") != Some(target.table.as_str())
                {
                    return Err(Anomaly::incorrect("rtdb.owned/spatial-path", "Spatial path belongs to another relation"));
                }
                present = true;
            }
        }
        if present { self.buffer.delete(catalog, &rtdb_types::EntityId::String(path.name().into()), Interval::ALL)?; }
        Ok(())
    }

    /// Fences the relation at this transaction: its paths leave the current catalogs, so no index serves it again.
    /// Its declaration stays, so historical reads inside the retention keep resolving its schema.
    /// Collection reclaims its tables once the retention has passed (gc.allium).
    async fn retire_owned_relation(&mut self, relation: &rtdb_types::OwnedRelation, retain_micros: i64) -> Result<()> {
        let owner = self.options.user.as_deref().filter(|owner| !owner.is_empty()).ok_or_else(no_owned_principal)?;
        if !relation.is_root_data() { return Err(rtdb_types::OwnedWorld::unavailable()); }
        let retirement = rtdb_types::OwnedRetirement::at(self.tx_key().system_time, retain_micros)?;
        let retired = rtdb_types::OwnedRelation::retired_catalog(owner);
        let snapshot = self.snapshot()?;
        let context = self.context.query_context(snapshot.clone(), self.tx_key().system_time, &self.options.default_tz, self.trace_parent)?;
        if snapshot.contains_table(&retired)
            && let Some(existing) = rtdb_query::read_owned_retirement(Arc::clone(&context), owner, relation).await?
        {
            return if existing.retain_micros == retain_micros { Ok(()) } else {
                Err(Anomaly::new(AnomalyCategory::Conflict, "rtdb.owned/relation-retired", "The owned relation is already retired with another retention"))
            };
        }
        let id = |name: &str| rtdb_types::EntityId::String(name.into());
        let target = relation.table(owner);
        for catalog in [
            rtdb_types::OwnedRelation::path_catalog(owner),
            rtdb_types::OwnedRelation::search_catalog(owner),
            rtdb_types::OwnedRelation::spatial_catalog(owner),
        ] {
            if !snapshot.contains_table(&catalog) { continue; }
            let Some(mut cursor) = rtdb_query::open_owned_path_catalog(Arc::clone(&context), &catalog, None).await? else { continue; };
            let mut paths = Vec::new();
            while let Some(batch) = cursor.next_batch().await? {
                let batch = batch.batch();
                for row in 0..batch.num_rows() {
                    let text = |field| ArrowCell::named(batch, field, row).and_then(ArrowCell::utf8);
                    if text("schema_name") == Some(target.schema.as_str()) && text("table_name") == Some(target.table.as_str())
                        && let Some(path) = text("_id")
                    {
                        paths.push(path.to_owned());
                    }
                }
            }
            for path in paths { self.buffer.delete(catalog.clone(), &id(&path), Interval::ALL)?; }
        }
        self.buffer.put(retired, retirement.record(relation), Interval::ALL)
    }

    /// No write or declaration reaches a relation this transaction or an earlier one retired.
    async fn check_unretired(&self, owner: &str, operations: &[Operation]) -> Result<()> {
        let retiring: BTreeSet<&str> = operations.iter().filter_map(|operation| match operation {
            Operation::Public(TxOp::OwnedRetireRelation { relation, .. }) => Some(relation.name()),
            _ => None,
        }).collect();
        let mut touched = BTreeSet::new();
        for operation in operations {
            if matches!(operation, Operation::Public(TxOp::OwnedRetireRelation { .. })) { continue; }
            let Some(relation) = operation.owned_relation() else { continue; };
            let root = relation.root_relation();
            let target = root.as_ref().unwrap_or(relation);
            if !target.is_root_data() { continue; }
            if retiring.contains(target.name()) { return Err(relation_retired()); }
            touched.insert(target.clone());
        }
        let snapshot = self.buffer.base_snapshot();
        if touched.is_empty() || !snapshot.contains_table(&rtdb_types::OwnedRelation::retired_catalog(owner)) { return Ok(()); }
        let context = self.context.query_context(snapshot, self.tx_key().system_time, &self.options.default_tz, self.trace_parent)?;
        for relation in touched {
            if rtdb_query::read_owned_retirement(Arc::clone(&context), owner, &relation).await?.is_some() {
                return Err(relation_retired());
            }
        }
        Ok(())
    }

    async fn declare_owned_path(
        &mut self, relation: &rtdb_types::OwnedRelation, name: &rtdb_types::OwnedRelation,
        columns: &[String], kind: OwnedPathKind,
    ) -> Result<()> {
        let owner = self.options.user.as_deref().filter(|owner| !owner.is_empty()).ok_or_else(no_owned_principal)?;
        if relation.world().is_some() || relation.is_world_catalog() { return Err(rtdb_types::OwnedWorld::unavailable()); }
        let target = relation.table(owner);
        if kind != OwnedPathKind::Ordered && self.owned_changes.get(relation.name()).is_some_and(|group| group.erased) {
            return Err(purge_path_uncovered());
        }
        let catalog = match kind {
            OwnedPathKind::Search => rtdb_types::OwnedRelation::search_catalog(owner),
            OwnedPathKind::Ordered => rtdb_types::OwnedRelation::path_catalog(owner),
            OwnedPathKind::Spatial(_) => rtdb_types::OwnedRelation::spatial_catalog(owner),
        };
        let declaration = self.read_owned_schema(owner, relation).await?;
        let snapshot = self.snapshot()?;
        if declaration.is_none() && !snapshot.contains_table(&target) {
            return Err(Anomaly::incorrect("rtdb.owned/path-relation", "Owned path requires a declared or stored relation"));
        }
        let declared_type = |name: &str| declaration.as_ref().and_then(|(schema, _)| schema.path_column_type(name));
        let available = snapshot.column_names(&target);
        if matches!(kind, OwnedPathKind::Spatial(_)) {
            if !(1..=16).contains(&columns.len())
                || columns.iter().collect::<std::collections::BTreeSet<_>>().len() != columns.len()
                || columns.iter().any(|column| column.starts_with('_') || declared_type(column) != Some(rtdb_types::ScalarType::Binary))
            {
                return Err(Anomaly::incorrect("rtdb.owned/spatial-path-columns", "Owned spatial paths require distinct declared Binary columns, geometry first"));
            }
        } else if kind == OwnedPathKind::Search {
            let [column] = columns else { return Err(invalid_search_path_column()); };
            let clear = if declaration.is_some() {
                declared_type(column) == Some(rtdb_types::ScalarType::Utf8)
            } else {
                let types = snapshot.column_types_for(&target, columns.iter().cloned());
                types.get(column).is_some_and(rtdb_types::LogicalType::is_clear_search_text)
            };
            if column.starts_with('_') || !clear {
                return Err(invalid_search_path_column());
            }
        } else if !(2..=16).contains(&columns.len())
            || columns.last().is_none_or(|column| column != "_id")
            || columns.iter().collect::<std::collections::BTreeSet<_>>().len() != columns.len()
            || columns[..columns.len() - 1].iter().any(|column| {
                column.starts_with('_') || if declaration.is_some() {
                    declared_type(column).is_none()
                } else {
                    !available.contains(column)
                }
            })
        {
            return Err(Anomaly::incorrect("rtdb.owned/path-columns", "Owned path requires distinct stored columns ending in _id"));
        }
        if snapshot.contains_table(&catalog) {
            let context = self.context.query_context(snapshot, self.tx_key().system_time, &self.options.default_tz, self.trace_parent)?;
            if let Some(mut existing) = rtdb_query::open_owned_path_catalog(context, &catalog, Some(name.name())).await? {
                let different = || Anomaly::new(AnomalyCategory::Conflict, "rtdb.owned/path-exists", "Owned path name has a different declaration");
                let mut found = false;
                while let Some(batch) = existing.next_batch().await? {
                    let batch = batch.batch();
                    for row in 0..batch.num_rows() {
                        if found { return Err(different()); }
                        let cell = |field| ArrowCell::named(batch, field, row);
                        let text = |field| cell(field).and_then(ArrowCell::utf8);
                        let stored_columns = cell("columns").and_then(ArrowCell::list).ok_or_else(different)?;
                        if text("_id") != Some(name.name())
                            || text("schema_name") != Some(target.schema.as_str())
                            || text("table_name") != Some(target.table.as_str())
                            || stored_columns.len() != columns.len()
                            || !stored_columns.zip(columns).all(|(value, expected)| value.utf8() == Some(expected.as_str()))
                        {
                            return Err(different());
                        }
                        if let OwnedPathKind::Spatial(expected) = kind {
                            let stored = rtdb_spatial::path_geometry_encoding(cell("geometry_encoding"))?;
                            if stored != expected { return Err(different()); }
                        }
                        found = true;
                    }
                }
                if found { return Ok(()); }
            }
        }
        use arrow_array::{ArrayRef, Int64Array, StringArray, StructArray, builder::{ListBuilder, StringBuilder}};
        let name_bytes = columns.iter().try_fold(0usize, |bytes, name| {
            bytes.checked_add(name.len()).ok_or_else(overflow)
        })?;
        let column_bytes = name_bytes.checked_add(columns.len() * 32 + 4096).ok_or_else(overflow)?;
        let _scratch = self.context.memory.reserve(column_bytes)?;
        let strings = StringBuilder::with_capacity(columns.len(), name_bytes);
        let mut column_names = ListBuilder::with_capacity(strings, 1);
        for column in columns {
            column_names.values().append_value(column);
        }
        column_names.append(true);
        let text = |value| Arc::new(StringArray::from(vec![value])) as ArrayRef;
        let mut columns = vec![
            ("_id", text(name.name())),
            ("schema_name", text(target.schema.as_str())),
            ("table_name", text(target.table.as_str())),
            ("columns", Arc::new(column_names.finish())),
            ("version", Arc::new(Int64Array::from(vec![self.tx_key().tx_id.0]))),
        ];
        if let OwnedPathKind::Spatial(encoding) = kind
            && encoding != rtdb_types::SpatialGeometryEncoding::TaggedWkb
        {
            columns.push(("geometry_encoding", text(encoding.name())));
        }
        let document = RecordBatch::try_from_iter(columns)
            .map_err(|error| Anomaly::fault("rtdb.owned/path-catalog", error.to_string()))?;
        let header = EventHeader {
            commit_position: 0,
            iid: rtdb_types::EntityId::String(name.name().into()).iid(),
            system_from: self.tx_key().system_time,
            valid_time: Interval::ALL,
            op: EventOp::Put,
        };
        let batch = rtdb_format::data::owned_put_batch(
            Arc::new(StructArray::from(document)), &[header], None, &self.context.memory,
        )?;
        self.buffer.append_batch(&catalog, batch)
    }

    async fn apply_constraint_refusal(
        &mut self,
        operations: &[Operation],
        scope: Option<rtdb_types::PlacedAttemptScope>,
    ) -> Result<()> {
        self.check_owned_preconditions(operations, scope, false).await?;
        for operation in operations {
            self.context.check_cancelled()?;
            validate_document_operation(operation, self.options.user.as_deref())?;
            self.apply(operation).await?;
        }
        self.finish_owned_unique().await?;
        self.finish_owned_capacity().await?;
        self.check_spatial_readiness(operations).await?;
        self.write_owned_changes().await
    }

    async fn check_spatial_readiness(&self, operations: &[Operation]) -> Result<()> {
        let mut context = None;
        for operation in operations {
            let Operation::Public(TxOp::OwnedRequireSpatialPath { relation, name, columns, encoding }) = operation
            else { continue; };
            let owner = self.options.user.as_deref().ok_or_else(no_owned_principal)?;
            let context = match &context {
                Some(context) => Arc::clone(context),
                None => {
                    let final_context = self.context.query_context(
                        self.snapshot()?, self.tx_key().system_time, &self.options.default_tz, self.trace_parent,
                    )?;
                    context = Some(Arc::clone(&final_context));
                    final_context
                }
            };
            let lease = context.catalog.read_lease();
            let check = rtdb_spatial::require_path_ready(Arc::clone(&context), owner, relation, name, columns, *encoding);
            rtdb_storage::IoRegion::retaining(context, rtdb_storage::IoRegion::leased(lease, check)).await?;
        }
        Ok(())
    }

    async fn check_owned_preconditions(
        &mut self, operations: &[Operation], source_scope: Option<rtdb_types::PlacedAttemptScope>, has_clock_slot: bool,
    ) -> Result<()> {
        let owner = self.options.user.as_deref().filter(|owner| !owner.is_empty()).ok_or_else(no_owned_principal)?;
        let replay = crate::hosted_control::hosted_replay_controls(operations.iter().filter_map(|op| match op {
            Operation::Public(TxOp::HostedControl(control)) => Some(control),
            _ => None,
        }))?;
        let mut deadline = None;
        for operation in operations {
            if let Operation::Public(TxOp::OwnedDeadline { until, .. }) = operation {
                if deadline.replace(*until).is_some() {
                    return Err(Anomaly::incorrect("rtdb.owned/duplicate-deadline", "An owned transaction may supply at most one deadline"));
                }
            }
        }
        if let Some(scope) = operations.iter().find_map(|operation| match operation {
            Operation::Public(TxOp::OwnedSweepExpired { scope, .. }) => Some(*scope),
            _ => None,
        }) {
            self.context.authorization.check_role_membership(&self.context.database, self.options.user.as_deref())?;
            if operations.len() != 2 || deadline.is_none() || source_scope != Some(scope) {
                return Err(lifetime_uncovered());
            }
        }
        self.check_unretired(owner, operations).await?;
        let base = self.buffer.base_snapshot();
        let new_lifetimes: BTreeSet<_> = operations.iter().filter_map(|operation| match operation {
            Operation::Public(TxOp::OwnedDeclareRelation { relation, schema }) if schema.lifetime_micros().is_some() => Some(relation),
            _ => None,
        }).collect();
        for operation in operations {
            let Some(relation) = operation.owned_relation() else { continue; };
            let root = relation.root_relation();
            let target = root.as_ref().unwrap_or(relation);
            if new_lifetimes.contains(target) || base.contains_table(&target.lifetime_table(owner)) {
                match operation {
                    Operation::Public(TxOp::OwnedErase { .. }) if replay.is_some() && root.is_none() => (),
                    Operation::Public(TxOp::OwnedDeclareRelation { .. } | TxOp::OwnedDeclarePath { .. }
                        | TxOp::OwnedInsertDocs { .. } | TxOp::OwnedCheck { .. } | TxOp::OwnedCheckRange { .. }
                        | TxOp::OwnedSweepExpired { .. } | TxOp::OwnedRequireSpatialPath { .. } | TxOp::OwnedRetireRelation { .. })
                    | Operation::Public(TxOp::OwnedArrowDocs { create_only: true, .. })
                    | Operation::OwnedArrowIids { create_only: true, .. }
                    | Operation::OwnedWriteIids { create_only: true, .. } if root.is_none() => (),
                    _ => return Err(lifetime_uncovered()),
                }
            }
        }
        let unavailable = rtdb_types::OwnedWorld::unavailable;
        let publications = operations.iter().filter_map(|operation| match operation {
            Operation::Public(TxOp::OwnedPublishWorld { world, parent_pin, attempt, fingerprint, advance_to, drop_world, settlement_pin, .. }) =>
                Some((*world, attempt, *fingerprint, *advance_to, *drop_world, *settlement_pin, *parent_pin)),
            _ => None,
        }).take(9).collect::<Vec<_>>();
        let mut root_publications = operations.iter().filter_map(|operation| match operation {
            Operation::Public(TxOp::OwnedPublishRoot { attempt, fingerprint }) => Some((attempt, *fingerprint)),
            _ => None,
        });
        let root_publication = root_publications.next();
        if root_publication.is_some() && (root_publications.next().is_some() || publications.is_empty()) {
            return Err(unavailable());
        }
        let publishing_worlds = publications.iter().map(|(world, ..)| *world).collect::<BTreeSet<_>>();
        let observing_worlds = independent_world_observations(operations, &publishing_worlds);
        let world_batch = publications.len() > 1 || root_publication.is_some()
            || (!publications.is_empty() && !observing_worlds.is_empty());
        let hosted_world_reads = crate::hosted_control::hosted_root_world_reads(operations);
        let root_conditions = !publications.is_empty() && operations.iter().any(|operation| match operation {
            Operation::Public(TxOp::OwnedCheck { relation, .. } | TxOp::OwnedCheckRange { relation, .. }
                | TxOp::OwnedCheckCapacity { relation, .. }) =>
                relation.world().is_none() && !relation.is_world_catalog(),
            _ => false,
        });
        if publications.len() + observing_worlds.len() > 8 { return Err(unavailable()); }
        if publications.is_empty() && operations.iter().any(|operation| match operation {
            Operation::Public(TxOp::OwnedInsertDocs { relation, .. } | TxOp::OwnedReplaceDocs { relation, .. })
            | Operation::OwnedWriteIids { relation, .. } => relation.world().is_some(),
            _ => false,
        }) {
            return Err(unavailable());
        }
        let mut scoped = operations.iter().filter_map(Operation::owned_relation);
        let mut admitted = BTreeSet::new();
        if let Some(first) = scoped.next() {
            if first.is_world_catalog() { return Err(unavailable()); }
            let world = first.world();
            if world_batch || root_conditions || hosted_world_reads {
                for relation in std::iter::once(first).chain(scoped) {
                    match relation.world() {
                        Some(world) => { admitted.insert(world); }
                        None if (hosted_world_reads || root_conditions || root_publication.is_some())
                            && !relation.is_world_catalog() => (),
                        None => return Err(unavailable()),
                    }
                    if admitted.len() > 8 { return Err(unavailable()); }
                }
            } else {
                if scoped.any(|relation| relation.world() != world || relation.is_world_catalog()) {
                    return Err(unavailable());
                }
                admitted.extend(world);
            }
        }
        let retained_release = crate::hosted_control::retained_release_world(operations);
        let proposal_overlay = crate::hosted_control::hosted_proposal_overlay(operations);
        let proposal_land = crate::hosted_control::hosted_proposal_land(operations);
        if !has_clock_slot && proposal_land.is_some_and(|land| {
            land.effect != crate::hosted_control::ProposalLandingEffect::Close
        }) {
            return Err(Anomaly::incorrect(
                "rtdb.owned/invalid-clock-slot", "Proposal landing requires its assigned transaction clock",
            ));
        }
        let mut clock_worlds = BTreeSet::new();
        for clock in operations.iter().filter_map(|operation| match operation {
            Operation::Public(TxOp::OwnedCheckWorldScope { clock: Some(clock), .. }) => Some(clock),
            _ => None,
        }) {
            if !(hosted_world_reads || world_batch) || publishing_worlds.contains(&clock.world)
                || !clock_worlds.insert(clock.world)
            {
                return Err(unavailable());
            }
            admitted.insert(clock.world);
            if admitted.len() > 8 {
                return Err(unavailable());
            }
        }
        if !publications.is_empty() && !observing_worlds.is_empty()
            && (proposal_land.is_some() || proposal_overlay.is_some() || retained_release.is_some())
        {
            return Err(unavailable());
        }
        if proposal_land.is_some() {
            self.buffer.retain_owned_positions()?;
        }
        if let Some(replay) = replay.as_ref().filter(|replay| !replay.commit_positions().is_empty()) {
            self.buffer.retain_archived_owned_positions(replay.commit_positions().to_vec())?;
        }
        admitted.extend(proposal_land.map(|landing| landing.world));
        if proposal_land.is_none() && operations.iter().any(|op| matches!(op,
            Operation::Public(TxOp::OwnedPublishWorld { land_root: Some(_), .. })
        )) {
            return Err(unavailable());
        }
        admitted.extend(retained_release);
        admitted.extend(proposal_overlay);
        admitted.extend(publishing_worlds);
        if proposal_overlay.is_none() && publications.iter().any(|publication| publication.5.is_some()) {
            return Err(unavailable());
        }
        if retained_release.is_none() && operations.iter().any(|op| matches!(
            op, Operation::Public(TxOp::OwnedPublishWorld { delete_root: Some(_), .. })
        )) {
            return Err(unavailable());
        }
        if !admitted.is_empty() {
            let context = self.context.query_context(self.snapshot()?, self.tx_key().system_time, &self.options.default_tz, self.trace_parent)?;
            for world in admitted {
                let record = rtdb_query::read_owned_world(Arc::clone(&context), owner, world).await?.ok_or_else(unavailable)?;
                if record.0.dropped || self.tx_key().system_time >= record.0.until {
                    return Err(unavailable());
                }
                self.owned_worlds.insert(world, record);
            }
        }
        if publications.iter().any(|(world, _, _, _, _, _, parent_pin)| {
            self.owned_worlds.get(world).is_none_or(|(record, _)| record.pin != *parent_pin)
        }) {
            return Err(unavailable());
        }
        if deadline.is_some_and(|until| self.tx_key().system_time >= until) {
            return Err(Anomaly::new(AnomalyCategory::Conflict, "rtdb.owned/expired", "Owned transaction reached its deadline before ordering"));
        }
        if let Some(root) = operations.iter().find_map(|operation| match operation {
            Operation::Public(TxOp::OwnedForkWorld { root, .. }) => Some(*root),
            _ => None,
        }) {
            use crate::OwnedForkRoot;
            let proposal_shorten = root == OwnedForkRoot::ProposalShorten;
            let proposal_hold = matches!(root, OwnedForkRoot::ProposalHold | OwnedForkRoot::ProposalShorten);
            let renewal = root == OwnedForkRoot::WritesRenewal || proposal_hold;
            let sibling_fork = root == OwnedForkRoot::WritesSibling;
            let replaces_pin = matches!(root, OwnedForkRoot::WritesAdvance | OwnedForkRoot::WritesRenewal | OwnedForkRoot::ProposalHold | OwnedForkRoot::ProposalShorten);
            let bounded_write = replaces_pin || sibling_fork;
            let publish_root = root == OwnedForkRoot::Writes || root == OwnedForkRoot::ProposalWrites || bounded_write;
            let with_root = root != OwnedForkRoot::None;
            let proposal_publication = matches!(root, OwnedForkRoot::ProposalControls | OwnedForkRoot::ProposalWrites);
            let mut held_publication = false;
            let mut forks = 0;
            let mut checks = 0;
            let mut root_rows = 0usize;
            let mut control_writes = 0usize;
            let mut existing_control_write = false;
            let mut replacement = None;
            let mut effects = 1usize + usize::from(publish_root); // Shared attempt and optional Root head.
            let mut expected_tip = None;
            let mut checked_tip = None;
            if !crate::hosted_control::root_partition_frontiers_match(operations) {
                return Err(unavailable());
            }
            for operation in operations {
                effects = effects.saturating_add(1);
                if publish_root && let Some(markers) = crate::hosted_control::root_partition_frontier_rows(operation) {
                    effects = effects.saturating_add(markers.rows().len() - 1);
                    continue;
                }
                if let Some((relation, rows)) = operation.owned_documents()
                    && publish_root && !bounded_write && relation.is_root_data() && rows > 0
                {
                    root_rows = root_rows.saturating_add(rows);
                    effects = effects.saturating_add(rows - 1);
                    continue;
                }
                match operation {
                    Operation::Public(TxOp::OwnedDeadline { .. }) => {}
                    Operation::Public(TxOp::HostedControl(control)) if control.is_check() => { checks += 1; }
                    Operation::Public(TxOp::HostedControl(control)) if with_root => {
                        control_writes += 1;
                        existing_control_write |= control.expected().is_some();
                    }
                    Operation::Public(TxOp::OwnedCheck { relation, .. } | TxOp::OwnedCheckRange { relation, .. }
                        | TxOp::OwnedCheckCapacity { relation, .. })
                        if with_root && relation.is_root_data() => {}
                    Operation::Public(TxOp::OwnedReplaceDocs { relation, documents, valid_from: None, valid_to: None })
                    | Operation::OwnedWriteIids { relation, documents, valid_from: None, valid_to: None, create_only: false, .. }
                        if replaces_pin && !proposal_hold && relation.is_root_data() && documents.rows().len() == 1
                            && replacement.is_none() => {
                        let id = crate::source::document_id(&documents.rows()[0], &self.context.memory)?;
                        replacement = Some((relation, id));
                        root_rows += 1;
                    }
                    Operation::Public(TxOp::OwnedInsertDocs { relation, documents, valid_from: None, valid_to: None })
                    | Operation::OwnedWriteIids { relation, documents, valid_from: None, valid_to: None, create_only: true, .. }
                        if (sibling_fork || (proposal_hold && !proposal_shorten)) && relation.is_root_data() && documents.rows().len() == 1
                            && replacement.is_none() => {
                        let id = crate::source::document_id(&documents.rows()[0], &self.context.memory)?;
                        replacement = Some((relation, id));
                        root_rows += 1;
                    }
                    Operation::Public(TxOp::OwnedArrowDocs { relation, documents, valid_from: None, valid_to: None, create_only })
                    | Operation::OwnedArrowIids { relation, documents, valid_from: None, valid_to: None, create_only, .. }
                        if relation.is_root_data() && documents.array().len() == 1 && replacement.is_none()
                            && if *create_only { sibling_fork || (proposal_hold && !proposal_shorten) }
                                else { replaces_pin && !proposal_hold } => {
                        replacement = Some((relation, documents.owned_id(0)?));
                        root_rows += 1;
                    }
                    Operation::Public(TxOp::OwnedDelete { relation, id, valid_from: None, valid_to: None })
                        if proposal_shorten && relation.is_root_data() && replacement.is_none() => {
                        replacement = Some((relation, id.clone()));
                        root_rows += 1;
                    }
                    Operation::Public(TxOp::OwnedDelete { relation, .. }) if publish_root && !bounded_write && relation.is_root_data() => {
                        root_rows = root_rows.saturating_add(1);
                    }
                    Operation::Public(TxOp::OwnedCheck { relation, id: rtdb_types::EntityId::String(id), valid_at, expected })
                        if relation.name() == rtdb_types::OWNED_ROOT_PUBLICATION_RELATION && id == "head"
                            && checked_tip.is_none() => {
                        checked_tip = Some((*valid_at, *expected));
                    }
                    Operation::Public(TxOp::OwnedForkWorld { record, previous, sibling, overlay_source, .. }) => {
                        let world = rtdb_types::OwnedWorld::from_record(record)?;
                        use rtdb_types::WorldClockMode;
                        held_publication = matches!(root, OwnedForkRoot::Controls | OwnedForkRoot::Writes)
                            && overlay_source.is_none() && world.is_held_root_fork();
                        let valid_mode = if proposal_publication || proposal_hold {
                            world.mode == WorldClockMode::Proposal
                        } else {
                            held_publication || world.mode == WorldClockMode::Retained
                        };
                        if world.dropped || world.basis.system_time >= self.tx_key().system_time
                            || deadline.is_none_or(|until| until > world.until)
                            || sibling.is_some() != sibling_fork
                            || (with_root && ((previous.is_some() != bounded_write)
                                || !valid_mode
                                || ((proposal_publication || bounded_write) && world.parent_publication != Some(world.basis.system_time))
                                || world.basis.tx_id >= self.tx_key().tx_id))
                        {
                            return Err(unavailable());
                        }
                        if let Some((source_id, source_pin)) = overlay_source {
                            if with_root || previous.is_some() || sibling.is_some()
                                || world.ancestor.as_ref().is_some_and(|ancestor|
                                    ancestor.basis.system_time >= self.tx_key().system_time
                                        || ancestor.basis.tx_id >= self.tx_key().tx_id)
                            {
                                return Err(unavailable());
                            }
                            let base = self.buffer.base_snapshot();
                            let context = self.context.query_context(base, self.tx_key().system_time, &self.options.default_tz, self.trace_parent)?;
                            let (source, registered, _charge) = rtdb_query::read_owned_world_with_revision(context, owner, *source_id)
                                .await?.ok_or_else(unavailable)?;
                            let admits_child = if world.ancestor.is_some() {
                                source.can_retain_overlay(&world)
                            } else {
                                source.can_retain_parent(&world)
                            };
                            if source.pin != *source_pin || !admits_child
                                || self.tx_key().system_time >= source.until
                                || world.fork_lineage != source.retained_child_lineage(&world, registered)?
                            {
                                return Err(unavailable());
                            }
                        } else if world.ancestor.is_some()
                            || (previous.is_none() && !world.fork_lineage.is_empty())
                        {
                            return Err(unavailable());
                        }
                        if let Some(previous) = previous {
                            let base = self.buffer.base_snapshot();
                            let context = self.context.query_context(base, self.tx_key().system_time, &self.options.default_tz, self.trace_parent)?;
                            let (prior, _charge) = rtdb_query::read_owned_world(context, owner, sibling.unwrap_or(world.id)).await?.ok_or_else(unavailable)?;
                            let fits = if sibling_fork {
                                deadline.is_some_and(|until| until.0 <= prior.until.0.saturating_sub(crate::SIBLING_PIN_MARGIN_MICROS))
                                    && prior.can_seed_sibling(&world)
                            } else if replaces_pin {
                                deadline.is_some_and(|until| until <= prior.until) && if proposal_shorten {
                                    prior.can_shorten_proposal(&world)
                                } else if proposal_hold {
                                    prior.can_hold_proposal(&world)
                                } else if renewal {
                                    prior.can_renew_to(&world)
                                } else {
                                    prior.can_advance_to(&world)
                                }
                            } else {
                                prior.can_move_to(&world)
                            };
                            if prior.pin != *previous || !fits {
                                return Err(unavailable());
                            }
                        } else {
                            if world.pin != world.id { return Err(unavailable()); }
                        }
                        if world.initial_clock() > world.inherited_clock() || (world.mode == rtdb_types::WorldClockMode::Proposal && !proposal_hold)
                            || (with_root && !renewal && !sibling_fork && !held_publication
                                && world.parent_publication == Some(world.basis.system_time))
                        {
                            expected_tip = Some((world.basis.system_time, world.parent_publication));
                        }
                        forks += 1;
                    }
                    _ => return Err(unavailable()),
                }
            }
            let replacement_checked = replacement.is_some_and(|(relation, id)| operations.iter().any(|operation| matches!(
                operation,
                Operation::Public(TxOp::OwnedCheck { relation: checked, id: observed, expected, .. })
                    if *checked == *relation && *observed == id && expected.is_none() == (sibling_fork || (proposal_hold && !proposal_shorten))
            )));
            if (bounded_write && (!replacement_checked || !existing_control_write || control_writes > (5 + usize::from(proposal_hold))))
                || forks != 1 || checks == 0 || expected_tip != checked_tip || effects > 4096
                || (publish_root && root_rows == 0)
                || ((held_publication || root == OwnedForkRoot::Controls || proposal_publication) && control_writes == 0)
            {
                return Err(unavailable());
            }
        }
        if let Some(world) = proposal_overlay {
            if self.owned_worlds.get(&world).is_none_or(|(record, _)| {
                record.mode != rtdb_types::WorldClockMode::Proposal
            }) {
                return Err(unavailable());
            }
        }
        let mut held_create_readings = BTreeMap::new();
        for operation in operations {
            let Some(write) = crate::hosted_control::proposal_overlay_write(operation) else { continue; };
            let world = write.relation.world().ok_or_else(unavailable)?;
            let root = write.relation.root_relation().ok_or_else(unavailable)?;
            let (record, _) = self.owned_worlds.get(&world).ok_or_else(unavailable)?;
            let bounded = record.schemas.relation(&root).is_some_and(|schema| schema.requires_bounded_period());
            let held = matches!(record.mode, rtdb_types::WorldClockMode::Held { .. });
            let publishes_overlay = publications.iter().any(|(published, _, _, advance, drop, settlement, _)| {
                *published == world && (advance.is_none() || held) && !*drop && settlement.is_none()
            });
            if !publishes_overlay
                || (!held && (bounded || write.create_only) && proposal_overlay != Some(world))
                || (proposal_overlay == Some(world) && bounded != (write.from.is_some() && write.to.is_some()))
            {
                return Err(unavailable());
            }
            if held && write.create_only {
                if !record.schemas.relation(&root).is_some_and(|schema| schema.has_acceptance_validity())
                    || write.from.is_some() || write.to.is_some()
                {
                    return Err(unavailable());
                }
                let reading = match held_create_readings.entry(world) {
                    std::collections::btree_map::Entry::Occupied(entry) => *entry.get(),
                    std::collections::btree_map::Entry::Vacant(entry) => *entry.insert(
                        self.owned_world_clock(owner, world).await?.unwrap_or(record.initial_clock()),
                    ),
                };
                for row in write.documents.rows() {
                    let id = crate::source::document_id(row, &self.context.memory)?;
                    if !operations.iter().any(|operation| matches!(operation,
                        Operation::Public(TxOp::OwnedCheck { relation, id: observed, valid_at, expected: None })
                            if relation == write.relation && observed == &id && *valid_at == reading
                    )) {
                        return Err(unavailable());
                    }
                }
            }
        }
        if let Some(crate::hosted_control::HostedProposalLanding { world, pin, effect }) = proposal_land {
            use crate::hosted_control::ProposalLandingEffect;
            let (record, _) = self.owned_worlds.get(&world).ok_or_else(unavailable)?;
            let parent = record.parent_publication.ok_or_else(unavailable)?;
            if record.mode != rtdb_types::WorldClockMode::Proposal || record.pin != pin
                || record.basis.system_time != parent || deadline.is_none()
                || operations.len().saturating_add(2) > 4096
                || operations.iter().filter(|op| matches!(op, Operation::Public(TxOp::OwnedCheckWorldScope { .. }))).count() != 1
                || (effect == ProposalLandingEffect::WithPoints && self.read_owned_revision(
                    owner,
                    &rtdb_types::OwnedRelation::new(rtdb_types::OWNED_ROOT_PUBLICATION_RELATION)?,
                    &rtdb_types::EntityId::String("head".into()),
                    Interval::ALL,
                ).await? != Some(parent))
            {
                return Err(unavailable());
            }
            for operation in operations {
                if let Operation::Public(TxOp::OwnedArrowDocs { relation, documents, create_only: true, .. })
                    | Operation::OwnedArrowIids { relation, documents, create_only: true, .. } = operation
                {
                    for row in 0..documents.row_count() {
                        let id = documents.owned_id(row)?;
                        self.require_world_create_absence(owner, relation, relation, &id).await?;
                    }
                    continue;
                }
                let (relation, documents) = match operation {
                    Operation::Public(TxOp::OwnedInsertDocs { relation, documents, .. })
                    | Operation::OwnedWriteIids { relation, documents, create_only: true, .. } =>
                        (relation, documents),
                    _ => continue,
                };
                for document in documents.rows() {
                    let id = crate::source::document_id(document, &self.context.memory)?;
                    self.require_world_create_absence(owner, relation, relation, &id).await?;
                }
            }
        } else if let Some(world) = retained_release {
            let original_pin = operations.iter().find_map(|op| match op {
                Operation::Public(TxOp::OwnedPublishWorld { delete_root, .. }) => *delete_root,
                _ => None,
            });
            let represented = operations.len().saturating_add(2);
            if represented > 4096 || deadline.is_none()
                || self.owned_worlds.get(&world).is_none_or(|(record, _)| {
                    record.mode != rtdb_types::WorldClockMode::Retained
                        || original_pin.is_some_and(|pin| pin != record.pin)
                })
                || operations.iter().filter(|op| matches!(op, Operation::Public(TxOp::OwnedCheckWorldScope { .. }))).count() != 1
            {
                return Err(unavailable());
            }
        } else if !publications.is_empty() {
            // One immutable attempt per world and one Root attempt and head when writing Root.
            let mut effects = publications.len() + 2 * usize::from(root_publication.is_some());
            let mut root_rows = 0usize;
            let mut scoped_checks = 0;
            let mut hosted = false;
            let mut counts: BTreeMap<Iid, (usize, usize)> = publications.iter()
                .map(|(world, ..)| (*world, (0, 0))).collect();
            if !crate::hosted_control::root_partition_frontiers_match(operations) {
                return Err(unavailable());
            }
            for (index, operation) in operations.iter().enumerate() {
                let size = match operation {
                    _ if root_publication.is_some() && crate::hosted_control::root_partition_frontier_rows(operation).is_some() => {
                        crate::hosted_control::root_partition_frontier_rows(operation).expect("matched frontier write").rows().len()
                    }
                    _ if root_publication.is_some() && operation.owned_documents().is_some_and(|(relation, rows)| relation.is_root_data() && rows != 0) => {
                        let (_, count) = operation.owned_documents().expect("matched document write");
                        root_rows += count;
                        count
                    }
                    Operation::Public(TxOp::OwnedDelete { relation, .. })
                        if root_publication.is_some() && relation.is_root_data() => {
                        root_rows += 1;
                        1
                    }
                    Operation::Public(TxOp::OwnedInsertDocs { relation, documents, .. } | TxOp::OwnedReplaceDocs { relation, documents, .. })
                    | Operation::OwnedWriteIids { relation, documents, .. }
                        if !documents.rows().is_empty() => {
                        let world = relation.world().ok_or_else(unavailable)?;
                        let count = documents.rows().len();
                        counts.entry(world).or_default().0 += count;
                        count
                    }
                    Operation::Public(TxOp::OwnedCheck { relation, .. } | TxOp::OwnedCheckRange { relation, .. })
                        if relation.world().is_none() && !relation.is_world_catalog() => 1,
                    Operation::Public(TxOp::OwnedCheck { relation, .. }) => {
                        if !index.checked_sub(1).is_some_and(|previous|
                            matches!(&operations[previous], Operation::Public(TxOp::OwnedCheckWorldPurge { relation: guarded, .. }) if guarded == relation))
                        {
                            return Err(unavailable());
                        }
                        1
                    }
                    Operation::Public(TxOp::OwnedCheckWorldPurge { relation, .. }) => {
                        let world = relation.world().ok_or_else(unavailable)?;
                        counts.entry(world).or_default().1 += 1;
                        1
                    }
                    Operation::Public(TxOp::HostedControl(control)) if control.is_check() || proposal_overlay.is_some() => {
                        hosted = true;
                        1
                    }
                    Operation::Public(TxOp::OwnedCheckWorldScope { clock, .. }) => {
                        scoped_checks += usize::from(clock.is_none());
                        1
                    }
                    Operation::Public(TxOp::OwnedDeadline { .. }
                        | TxOp::OwnedPublishWorld { .. } | TxOp::OwnedPublishRoot { .. }) => 1,
                    _ => return Err(unavailable()),
                };
                effects = effects.saturating_add(size);
                if effects > 4096 { return Err(unavailable()); }
            }
            if deadline.is_none() || scoped_checks != 1 || !hosted
                || self.owned_worlds.len() != publications.len() + observing_worlds.len()
            {
                return Err(unavailable());
            }
            let fingerprint = publications[0].2;
            let mut attempts = BTreeSet::new();
            if let Some((attempt, root_fingerprint)) = root_publication {
                if root_rows == 0 || root_fingerprint != fingerprint {
                    return Err(unavailable());
                }
                attempts.insert(attempt);
            }
            for (world, attempt, own_fingerprint, advance_to, drop_world, settlement_pin, _) in publications {
                let (rows, witnesses) = counts.remove(&world).ok_or_else(unavailable)?;
                let proposal_drop = drop_world && self.owned_worlds.get(&world).is_none_or(|(record, _)|
                    record.mode == rtdb_types::WorldClockMode::Proposal);
                if (witnesses == 0 && (proposal_drop || (proposal_overlay.is_some() && settlement_pin.is_none())))
                    || witnesses > 128 || !attempts.insert(attempt) || own_fingerprint != fingerprint
                    || (rows == 0 && advance_to.is_none() && !drop_world && settlement_pin.is_none())
                    || (rows != 0 && (drop_world || settlement_pin.is_some()))
                    || drop_world && advance_to.is_some()
                {
                    return Err(unavailable());
                }
                if let Some(at) = advance_to.filter(|_| rows != 0) {
                    let (record, _) = self.owned_worlds.get(&world).ok_or_else(unavailable)?;
                    if !matches!(record.mode, rtdb_types::WorldClockMode::Held { .. })
                        || !record.admits_clock_publication(at, self.tx_key().system_time)
                    {
                        return Err(unavailable());
                    }
                    self.timed_world_overlays.insert(world, at);
                }
            }
            if counts.iter().any(|(world, (rows, witnesses))| {
                !observing_worlds.contains(world) || *rows != 0 || !(1..=128).contains(witnesses)
            }) {
                return Err(unavailable());
            }
        }
        self.check_hosted_controls(operations, source_scope, replay).await?;
        self.check_capacity_source(operations).await?;
        let publishes_catalog = operations.iter().any(|operation| matches!(operation,
            Operation::Public(TxOp::HostedControl(control)) if control.replaces_root_catalog()
        ));
        for operation in operations {
            if let Operation::Public(TxOp::OwnedRequireSpatialPath { relation, .. }) = operation
                && (!relation.is_root_data() || !publishes_catalog || source_scope.is_none()
                    || source_scope != self.context.owned_witness_scope)
            {
                return Err(unavailable());
            }
        }
        for operation in operations {
            if let Operation::Public(TxOp::OwnedCheckWorldScope { scope, clock }) = operation {
                if self.owned_worlds.is_empty() || deadline.is_none()
                    || Some(*scope) != source_scope || Some(*scope) != self.context.owned_witness_scope
                {
                    return Err(Anomaly::new(AnomalyCategory::Conflict, "rtdb.owned/world-witness-unavailable",
                        "Owned world point witness is no longer admissible"));
                }
                if let Some(clock) = clock {
                    let (world, _) = self.owned_worlds.get(&clock.world).ok_or_else(unavailable)?;
                    if world.pin != clock.pin { return Err(unavailable()); }
                    use rtdb_types::WorldClockMode;
                    let reading = match world.mode {
                        WorldClockMode::Retained => world.initial_clock(),
                        WorldClockMode::Held { .. } => self.owned_world_clock(owner, world.id).await?.unwrap_or(world.initial_clock()),
                        WorldClockMode::Proposal => self.tx_key().system_time,
                    };
                    if !clock.admits(world, reading) {
                        return Err(Anomaly::new(AnomalyCategory::Conflict, "rtdb.owned/world-clock-expired",
                            "The owned world reached its exclusive clock bound"));
                    }
                }
                continue;
            }
            if let Operation::Public(TxOp::OwnedCheckWorldPurge { relation, pin, frontier, local_frontier }) = operation {
                let root = relation.root_relation().ok_or_else(unavailable)?;
                let base = self.buffer.base_snapshot();
                if relation.world().and_then(|world| self.owned_worlds.get(&world))
                    .is_none_or(|(record, _)| record.pin != *pin || (local_frontier.is_some() && !record.mode.is_writable()))
                    || *frontier < 0
                    || base.contains_table(&rtdb_types::OwnedRelation::obsolete_purge_table(owner))
                    || base.relation_event_frontier(&root.purge_table(owner))? != *frontier
                {
                    return Err(Anomaly::new(AnomalyCategory::Conflict, "rtdb.owned/world-witness-unavailable",
                        "The world point's retained parent or root erasure frontier changed before ordering"));
                }
                if let Some(expected) = local_frontier
                    && base.relation_event_frontier(&relation.table(owner))? != *expected
                {
                    return Err(Anomaly::new(AnomalyCategory::Conflict, "rtdb.owned/world-witness-unavailable",
                        "The world overlay changed before ordering"));
                }
                continue;
            }
            if let Operation::Public(TxOp::OwnedCheckRange { relation, basis, scope, frontier, until, region }) = operation {
                let base = self.buffer.base_snapshot();
                let unavailable = || Anomaly::new(AnomalyCategory::Conflict, "rtdb.owned/range-witness-unavailable",
                    "Owned range witness is no longer admissible");
                if Some(*scope) != source_scope || Some(*scope) != self.context.owned_witness_scope
                    || self.tx_key().system_time >= *until || *frontier < 0 || basis.tx_id.0 < 0
                    || relation.world().is_some() || relation.is_world_catalog()
                    || base.tx_basis().is_none_or(|head| basis.tx_id > head.tx_id || basis.system_time > head.system_time
                        || ((basis.tx_id == head.tx_id) != (basis.system_time == head.system_time)))
                {
                    return Err(unavailable());
                }
                let head = base.relation_event_frontier(&relation.table(owner))?;
                if head != *frontier {
                    let exact = match region.as_deref() {
                        Some(rtdb_types::OwnedRangeEvidence::FixedPath(evidence)) =>
                            self.verify_fixed_range(&base, owner, relation, basis.system_time, *frontier, head, evidence).await,
                        Some(rtdb_types::OwnedRangeEvidence::Entity { anchor, region }) =>
                            self.verify_owned_entity_region(&base, owner, relation, basis.system_time, *frontier, head, *anchor, region).await,
                        None => Ok(false),
                    }.unwrap_or(false);
                    if !exact {
                        return Err(Anomaly::new(AnomalyCategory::Conflict, "rtdb.owned/stale-range",
                            "The observed owned relation changed before ordering"));
                    }
                }
                continue;
            }
            let Operation::Public(TxOp::OwnedCheck { relation, id, valid_at, expected }) = operation else { continue; };
            let observed = if self.buffer.base_snapshot().contains_table(&relation.lifetime_table(owner)) {
                let accepted = self.tx_key().system_time;
                if *valid_at >= accepted {
                    return Err(lifetime_uncovered());
                }
                // Expiration does not publish a new row revision. Certify
                // visibility again at acceptance, not at the old valid instant.
                accepted
            } else {
                *valid_at
            };
            let upper = observed.0.checked_add(1).map(Timestamp)
                .ok_or_else(|| Anomaly::incorrect("rtdb.owned/invalid-read-instant", "The observation's valid instant has no following microsecond"))?;
            let actual = self.read_owned_revision(owner, relation, id, Interval::new(observed, upper)?).await?;
            if actual != *expected {
                return Err(Anomaly::new(AnomalyCategory::Conflict, "rtdb.owned/stale-point", "The owned point observation changed before ordering"));
            }
        }
        Ok(())
    }

    async fn read_owned_revision(&self, owner: &str, relation: &rtdb_types::OwnedRelation, id: &rtdb_types::EntityId, valid: Interval) -> Result<Option<Timestamp>> {
        let context = self.context.query_context(self.snapshot()?, self.tx_key().system_time, &self.options.default_tz, self.trace_parent)?;
        rtdb_query::read_owned_point_revision(context, owner, relation, id, valid).await
    }

    async fn require_world_create_absence(
        &self, owner: &str, root: &rtdb_types::OwnedRelation, relation: &rtdb_types::OwnedRelation, id: &rtdb_types::EntityId,
    ) -> Result<()> {
        let context = self.context.query_context(
            self.snapshot()?, self.tx_key().system_time, &self.options.default_tz, self.trace_parent,
        )?;
        if rtdb_query::owned_purge_floor(context, owner, root, id).await?.is_some()
            || self.read_owned_revision(owner, relation, id, Interval::ALL).await?.is_some()
        {
            return Err(insert_exists());
        }
        Ok(())
    }

    async fn owned_world_clock(&self, owner: &str, world: Iid) -> Result<Option<Timestamp>> {
        let context = self.context.query_context(
            self.buffer.base_snapshot(), self.tx_key().system_time, &self.options.default_tz, self.trace_parent,
        )?;
        rtdb_query::read_owned_world_clock(context, owner, world).await
    }

    async fn held_overlay_reading(
        &self, owner: &str, world: &rtdb_types::OwnedWorld,
    ) -> Result<Timestamp> {
        if let Some(reading) = self.timed_world_overlays.get(&world.id) {
            return Ok(*reading);
        }
        self.owned_world_clock(owner, world.id).await?.unwrap_or(world.initial_clock()).next()
    }

    async fn publish_owned_world(
        &mut self, world: Iid, parent_pin: Iid, attempt: &str, fingerprint: [u8; 32], expected: Option<Timestamp>,
        advance_to: Option<Timestamp>, drop_world: bool, settlement_pin: Option<Iid>,
    ) -> Result<()> {
        use rtdb_types::{OwnedRelation, OwnedWorld, WorldClockMode};
        let owner = self.options.user.as_deref().filter(|owner| !owner.is_empty()).ok_or_else(no_owned_principal)?;
        let record = &self.owned_worlds.get(&world).ok_or_else(OwnedWorld::unavailable)?.0;
        if record.pin != parent_pin || (!record.mode.is_writable() && !drop_world) {
            return Err(OwnedWorld::unavailable());
        }
        let initial_clock = record.initial_clock();
        let previous = self.owned_world_clock(owner, world).await?;
        if previous != expected || previous.is_some_and(|at| !record.mode.is_writable() || at <= initial_clock) {
            return Err(Anomaly::new(AnomalyCategory::Conflict, "rtdb.world/stale-clock", "The hosted world application clock changed"));
        }
        if let Some(pin) = settlement_pin {
            if record.mode != WorldClockMode::Proposal || record.pin != pin || drop_world || advance_to.is_some() {
                return Err(OwnedWorld::unavailable());
            }
            return self.reserve_owned_attempt(attempt, fingerprint).await;
        }
        let head = previous.unwrap_or(initial_clock);
        let next = if drop_world {
            None
        } else {
            Some(match (record.mode, advance_to) {
                (WorldClockMode::Held { .. }, Some(to)) => to,
                (WorldClockMode::Held { .. }, None) => head.next()?,
                (WorldClockMode::Proposal, None) => self.tx_key().system_time,
                _ => return Err(OwnedWorld::unavailable()),
            })
        };
        if next.is_some_and(|next| next <= head || !record.admits_clock_publication(next, self.tx_key().system_time)) {
            return Err(Anomaly::new(AnomalyCategory::Conflict, "rtdb.world/clock-bound",
                "A world publication requires a strictly increasing finite reading"));
        }

        self.reserve_owned_attempt(attempt, fingerprint).await?;
        if let Some(next) = next {
            return self.buffer.put(OwnedRelation::world_clock_table(owner), StructFields::from([
                ("_id".into(), Value::Uuid(world.0)), ("clock".into(), Value::I64(next.0)),
            ]), Interval::ALL);
        }
        let mut terminal = self.owned_worlds.get(&world).ok_or_else(OwnedWorld::unavailable)?.0.record();
        terminal.insert("dropped".into(), Value::Bool(true));
        let documents = crate::Documents::new(vec![terminal], &self.context.memory)?;
        self.write_owned(&OwnedRelation::world_catalog(), &documents, None, None, None, false).await
    }

    async fn publish_owned_root_head(&mut self) -> Result<()> {
        let relation = rtdb_types::OwnedRelation::new(rtdb_types::OWNED_ROOT_PUBLICATION_RELATION)?;
        let documents = crate::Documents::new(vec![StructFields::from([
            ("_id".into(), Value::Utf8("head".into())),
        ])], &self.context.memory)?;
        self.write_owned(&relation, &documents, None, Some(Timestamp::MIN), Some(Timestamp::MAX), false).await
    }

    async fn reserve_owned_attempt(&mut self, attempt: &str, fingerprint: [u8; 32]) -> Result<()> {
        use rtdb_types::{EntityId, OWNED_ATTEMPT_RELATION, OwnedRelation, OwnedWorld};
        let owner = self.options.user.as_deref().filter(|owner| !owner.is_empty()).ok_or_else(no_owned_principal)?;
        if attempt.len() != 22 || !attempt.bytes().all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte)) {
            return Err(OwnedWorld::unavailable());
        }
        let relation = OwnedRelation::new(OWNED_ATTEMPT_RELATION)?;
        let id = EntityId::String(attempt.to_owned());
        if self.read_owned_revision(owner, &relation, &id, Interval::ALL).await?.is_some() {
            return Err(Anomaly::new(AnomalyCategory::Conflict, "rtdb.world/context-used", "The attempt context has already published"));
        }
        let docs = crate::Documents::new(vec![StructFields::from([
            ("_id".into(), id.as_value()), ("fingerprint".into(), Value::Binary(fingerprint.to_vec())),
        ])], &self.context.memory)?;
        self.write_owned(&relation, &docs, None, Some(Timestamp::MIN), Some(Timestamp::MAX), true).await
    }

    async fn sweep_owned_expiry(
        &mut self, relation: &rtdb_types::OwnedRelation, id: &rtdb_types::EntityId,
        created: Timestamp, nominated_expiry: Timestamp,
    ) -> Result<()> {
        use rtdb_types::{OwnedChangeKind, OwnedLifetimeFloor};
        let owner = self.options.user.as_deref().filter(|owner| !owner.is_empty()).ok_or_else(no_owned_principal)?;
        let ordered = self.tx_key().system_time;
        if !relation.is_root_data() { return Err(lifetime_uncovered()); }
        let lifetime = self.read_owned_schema(owner, relation).await?
            .and_then(|(schema, _)| schema.lifetime_micros()).ok_or_else(lifetime_uncovered)?;
        let expires = Timestamp(created.0.checked_add(lifetime).ok_or_else(overflow)?);
        let cutoff = OwnedLifetimeFloor::expiry_replay_cutoff(ordered);
        if expires != nominated_expiry || expires > cutoff { return Err(lifetime_uncovered()); }
        let snapshot = self.snapshot()?;
        if snapshot.contains_table(&relation.fixed_range_table(owner)) {
            return Err(purge_path_uncovered());
        }
        let context = self.context.query_context(snapshot, ordered, &self.options.default_tz, self.trace_parent)?;
        let mut floor = rtdb_query::read_owned_lifetime_floor(Arc::clone(&context), owner, relation).await?;
        floor.check(lifetime, ordered)?;
        if rtdb_query::owned_purge_floor(Arc::clone(&context), owner, relation, id).await?.is_some() {
            return Ok(());
        }
        if self.observed_owned_lifetime(context, owner, relation, id, lifetime).await?
            != Some((created, expires))
        {
            return Err(lifetime_uncovered());
        }
        floor.expired_created_through = OwnedLifetimeFloor::at(lifetime, ordered)?.expired_created_through;
        floor.swept_present_through = floor.swept_present_through.max(cutoff);
        let original = rtdb_types::expired_lifetime_interval(created, expires)?;
        self.erase_owned(relation, id, OwnedChangeKind::Expired(original)).await?;
        self.buffer.put(relation.lifetime_table(owner), floor.record(relation), Interval::ALL)?;
        Ok(())
    }

    async fn observed_owned_lifetime(
        &self,
        context: Arc<rtdb_query::QueryContext>,
        owner: &str,
        relation: &rtdb_types::OwnedRelation,
        id: &rtdb_types::EntityId,
        duration: i64,
    ) -> Result<Option<(Timestamp, Timestamp)>> {
        let columns = ["_id", "_system_from", "_valid_from", "_valid_to"];
        let Some(mut point) = rtdb_query::open_owned_point(
            context, owner, relation, id, Some(Interval::ALL), Some(&columns),
        ).await? else { return Ok(None); };
        let mut observed = None;
        while let Some(batch) = point.next_batch().await? {
            for row in 0..batch.batch().num_rows() {
                if observed.is_some() { return Err(lifetime_uncovered()); }
                let revision = rtdb_query::owned_row_revision(batch.batch(), row, id, &self.context.memory)?;
                observed = Some(rtdb_query::owned_row_lifetime(batch.batch(), row, revision, duration)?);
            }
        }
        Ok(observed)
    }

    async fn erase_owned(
        &mut self, relation: &rtdb_types::OwnedRelation, id: &rtdb_types::EntityId,
        kind: rtdb_types::OwnedChangeKind,
    ) -> Result<()> {
        use rtdb_types::OwnedRelation;
        if relation.name() == rtdb_types::OWNED_ATTEMPT_RELATION {
            return Err(Anomaly::incorrect("rtdb.owned/immutable-attempt", "Attempt reservations are immutable"));
        }
        let options = self.options;
        let owner = options.user.as_deref().filter(|owner| !owner.is_empty()).ok_or_else(no_owned_principal)?;
        if relation.world().is_some() || relation.is_world_catalog() {
            return Err(rtdb_types::OwnedWorld::unavailable());
        }
        let replay_lifetime = if self.hosted_replay.is_some() {
            let (schema, _) = self.read_owned_schema(owner, relation).await?.ok_or_else(replay_period_unavailable)?;
            schema.lifetime_micros().map(|duration| {
                let replay = self.hosted_replay.as_mut().ok_or_else(lifetime_uncovered)?;
                let floor = replay.lifetime(relation).ok_or_else(lifetime_uncovered)?;
                floor.check(duration, replay.source_time())?;
                let original = replay.take_expiry(relation, id)?;
                Ok::<_, Anomaly>((duration, original))
            }).transpose()?
        } else { None };
        let snapshot = self.snapshot()?;
        if snapshot.contains_table(&OwnedRelation::obsolete_purge_table(owner)) {
            return Err(Anomaly::new(AnomalyCategory::Unavailable, "rtdb.owned/purge-history-reset", "Owner-wide purge history requires a reset before native erasure"));
        }
        if snapshot.contains_table(&relation.unique_table(owner))
            || snapshot.contains_table(&relation.capacity_table(owner)) || self.has_staged_capacity(relation)
        {
            return Err(purge_path_uncovered());
        }
        let table = relation.table(owner);
        let ordered = self.tx_key();
        let context = self.context.query_context(snapshot, ordered.system_time, &self.options.default_tz, self.trace_parent)?;
        let kind = if let Some((duration, original)) = replay_lifetime {
            if self.observed_owned_lifetime(Arc::clone(&context), owner, relation, id, duration)
                .await?.is_some_and(|(created, expires)| created != original.lower() || expires != original.upper())
            {
                return Err(lifetime_uncovered());
            }
            rtdb_types::OwnedChangeKind::Expired(original)
        } else { kind };
        let search = OwnedRelation::search_catalog(owner);
        if let Some(mut rows) = rtdb_query::open_owned_path_catalog(Arc::clone(&context), &search, None).await? {
            while let Some(batch) = rows.next_batch().await? {
                let batch = batch.batch();
                for row in 0..batch.num_rows() {
                    let read = |name| {
                        let (slot, field) = batch.schema_ref().column_with_name(name).ok_or_else(purge_path_uncovered)?;
                        let array = batch.column(slot).as_ref();
                        let bound = rtdb_columnar::estimate_read_value_bytes(field, array, row)?;
                        let _charge = self.context.memory.reserve(bound.saturating_add(4096))?;
                        rtdb_columnar::read_value(field, array, row)
                    };
                    let (Value::Utf8(schema), Value::Utf8(name)) = (read("schema_name")?, read("table_name")?) else {
                        return Err(purge_path_uncovered());
                    };
                    if table.schema == schema && table.table == name { return Err(purge_path_uncovered()); }
                }
            }
        }
        let mut bucket = rtdb_query::read_owned_purge_bucket(Arc::clone(&context), owner, relation, id).await?;
        // The full handle, including its type, is the erasure target even
        // when the locality prefix is shared or the key has no live winner.
        let first_erase = !self.owned_changes.get(relation.name()).is_some_and(|group| group.erased);
        let fixed_before = self.fixed_range_before(owner, relation, id, Interval::ALL).await;
        let position = self.buffer.append_to(&table, StoredEvent {
            origin: None,
            commit_position: 0,
            entity_key: Some(id.handle_bytes().into()), iid: id.iid(),
            system_from: self.tx_key().system_time, valid_time: Interval::ALL,
            operation: StoredOperation::Erase, recency: None,
        })?;
        let fixed = self.fixed_range_pair(id, fixed_before, |_| Ok(None));
        self.record_owned_change(relation, id.clone(), kind, Interval::ALL, position, fixed)?;
        let floors = relation.purge_floor_table(owner);
        self.buffer.put(floors.clone(), bucket.advance(id, ordered.system_time)?, Interval::ALL)?;
        if first_erase {
            let markers = relation.purge_table(owner);
            let through = self.snapshot()?.relation_event_frontier(&markers)?
                .checked_add(1).ok_or_else(overflow)?;
            self.buffer.put(markers, StructFields::from([
                ("_id".into(), Value::I64(ordered.tx_id.0)),
            ]), Interval::ALL)?;
            self.buffer.put(floors, rtdb_query::purge_coverage_document(through), Interval::ALL)?;
        }
        Ok(())
    }

    fn reserve_bounded_period_write(
        &mut self, relation: &rtdb_types::OwnedRelation, id: rtdb_types::EntityId,
    ) -> Result<()> {
        if relation.is_root_data() && self.buffer.retains_owned_positions() {
            return Ok(());
        }
        let bytes = id.as_value().retained_bytes()?.checked_mul(2)
            .and_then(|bytes| bytes.checked_add(1024)).ok_or_else(overflow)?;
        let key = (relation.clone(), id);
        match self.bounded_period_writes.entry(key) {
            std::collections::btree_map::Entry::Vacant(entry) => {
                entry.insert(self.context.memory.reserve(bytes)?);
                Ok(())
            }
            std::collections::btree_map::Entry::Occupied(_) => Err(Anomaly::incorrect(
                "rtdb.owned/repeated-period",
                "One ordered transaction may assert one period per complete bounded-period key",
            )),
        }
    }

    async fn delete_owned(&mut self, relation: &rtdb_types::OwnedRelation, id: &rtdb_types::EntityId, from: Option<Timestamp>, to: Option<Timestamp>) -> Result<()> {
        if relation.name() == rtdb_types::OWNED_ATTEMPT_RELATION {
            return Err(Anomaly::incorrect("rtdb.owned/immutable-attempt", "Attempt reservations are immutable"));
        }
        let options = self.options;
        let owner = options.user.as_deref().filter(|owner| !owner.is_empty()).ok_or_else(no_owned_principal)?;
        if relation.world().is_some() || relation.is_world_catalog() { return Err(rtdb_types::OwnedWorld::unavailable()); }
        let selected_valid = if let Some(source) = self.hosted_replay.as_ref().map(|replay| replay.source_time()) {
            let (schema, _retained) = self.read_owned_schema(owner, relation).await?.ok_or_else(replay_period_unavailable)?;
            let valid = replay_period(from, to)?;
            schema.require_replayed_period(valid, source, EventOp::Delete)?;
            if schema.requires_bounded_period() {
                self.reserve_bounded_period_write(relation, id.clone())?;
            }
            valid
        } else {
            if let Some((schema, _retained)) = self.read_owned_schema(owner, relation).await? {
                schema.require_root_write_period(from, to)?;
                if schema.requires_bounded_period() {
                    self.reserve_bounded_period_write(relation, id.clone())?;
                }
            }
            interval(from.unwrap_or(self.tx_key().system_time), to.unwrap_or(Timestamp::MAX))?
        };
        let valid = selected_valid;
        self.read_owned_revision(owner, relation, id, Interval::ALL).await?;
        self.track_unique_mutation(owner, relation, id, valid).await?;
        self.track_capacity_mutation(owner, relation, id, valid).await?;
        let fixed_before = self.fixed_range_before(owner, relation, id, valid).await;
        let system_from = self.tx_key().system_time;
        let position = self.buffer.append_to(&relation.table(owner), StoredEvent { origin: None, commit_position: 0, entity_key: Some(id.handle_bytes().into()),
            iid: id.iid(), system_from, valid_time: valid, operation: StoredOperation::Delete, recency: None,
        })?;
        let fixed = self.fixed_range_pair(id, fixed_before, |_| Ok(None));
        self.record_owned_change(relation, id.clone(), rtdb_types::OwnedChangeKind::Delete, valid, position, fixed)
    }

    async fn write_owned(&mut self, relation: &rtdb_types::OwnedRelation, documents: &crate::Documents, iids: Option<&[Iid]>, from: Option<Timestamp>, to: Option<Timestamp>, create_only: bool) -> Result<()> {
        if relation.name() == rtdb_types::OWNED_ATTEMPT_RELATION
            && (!create_only || from != Some(Timestamp::MIN) || to.is_some_and(|to| to != Timestamp::MAX)
                || documents.rows().iter().any(|row| row.len() != 2
                    || !matches!(row.get("_id"), Some(Value::Utf8(_)))
                    || !matches!(row.get("fingerprint"), Some(Value::Binary(bytes)) if bytes.len() == 32)))
        {
            return Err(Anomaly::incorrect("rtdb.owned/immutable-attempt", "Attempt reservations require complete immutable fingerprints"));
        }
        let options = self.options;
        let owner = options.user.as_deref().filter(|owner| !owner.is_empty()).ok_or_else(no_owned_principal)?;
        let table = relation.table(owner);
        if let Some(iids) = iids {
            if iids.len() != documents.rows().len() {
                return Err(Anomaly::incorrect("xtdb/invalid-iids", "Document and IID counts differ"));
            }
            for (document, iid) in documents.rows().iter().zip(iids) {
                if crate::source::document_id(document, &self.context.memory)?.iid() != *iid {
                    return Err(Anomaly::incorrect("xtdb/invalid-iids", "Owned document IID does not match its identity"));
                }
            }
        }
        let uses_ordered_micros = documents.columns().iter().any(|name| name == ordered_micros::FIELD);
        if uses_ordered_micros && (!relation.is_root_data()
            || self.context.owned_witness_scope.is_none() || self.hosted_replay.is_some()
            || !documents.has_exact_presence())
        {
            return Err(ordered_micros::invalid());
        }
        if relation.is_world_catalog() {
            let unavailable = rtdb_types::OwnedWorld::unavailable;
            if from.is_some() || to.is_some() || documents.rows().len() != 1 || !documents.has_exact_presence() {
                return Err(unavailable());
            }
            let record = rtdb_types::OwnedWorld::from_record(&documents.rows()[0])?;
            let ordered = self.tx_key().system_time;
            if create_only {
                if record.dropped || record.basis.system_time >= ordered || record.until <= ordered {
                    return Err(unavailable());
                }
            } else {
                let context = self.context.query_context(self.snapshot()?, ordered, &self.options.default_tz, self.trace_parent)?;
                let (previous, _retained) = rtdb_query::read_owned_world(context, owner, record.id).await?.ok_or_else(unavailable)?;
                if record.dropped {
                    if !previous.same_fork(&record) { return Err(unavailable()); }
                    if previous.dropped { return Ok(()); }
                } else if !previous.can_move_to(&record) {
                    return Err(unavailable());
                }
            }
            return self.put_documents(&table, documents, iids, Some(Timestamp::MIN), Some(Timestamp::MAX),
                DocumentWrite::Owned { relation, owner, create_only }).await;
        }
        if let Some(root) = relation.root_relation() {
            use rtdb_types::WorldClockMode;

            let snapshot = self.snapshot()?;
            let unavailable = rtdb_types::OwnedWorld::unavailable;
            let world = relation.world().ok_or_else(unavailable)?;
            let (record, _) = self.owned_worlds.get(&world).ok_or_else(unavailable)?;
            let schema = record.schemas.relation(&root)
                .ok_or_else(|| Anomaly::incorrect("rtdb.owned/world-undeclared", "World writes require a schema retained from the parent cut"))?;
            if schema.lifetime_micros().is_some() { return Err(lifetime_uncovered()); }
            let (from, to, explicit) = if matches!(record.mode, WorldClockMode::Held { .. }) && schema.has_acceptance_validity() {
                let accepted = self.held_overlay_reading(owner, record).await?;
                if (from, to) != (None, None) && (from, to) != (Some(accepted), Some(Timestamp::MAX)) {
                    return Err(Anomaly::incorrect(
                        "rtdb.owned/validity-policy",
                        "This held relation requires validity from its ordered world clock with an open end",
                    ));
                }
                (accepted, Timestamp::MAX, false)
            } else {
                match (record.mode, from, to) {
                    (mode, Some(from), Some(to)) if mode.is_writable() => (from, to, true),
                    (WorldClockMode::Proposal, None, None) => (self.tx_key().system_time, Timestamp::MAX, false),
                    _ => return Err(unavailable()),
                }
            };
            if (create_only && (!record.mode.is_writable() || explicit))
                || from >= to || !documents.has_exact_presence()
            {
                return Err(unavailable());
            }
            if schema.requires_bounded_period() {
                schema.require_root_write_period(Some(from), Some(to))?;
            } else {
                schema.require_write_validity(explicit)?;
            }
            for row in documents.rows() {
                let _scratch = self.context.memory.reserve(Value::document_retained_bytes(row)?
                    .checked_mul(2).and_then(|bytes| bytes.checked_add(4096)).ok_or_else(overflow)?)?;
                let id = crate::source::document_id(row, &self.context.memory)?;
                let shadow = rtdb_types::WorldShadow::from_record(row.clone())?;
                match &shadow {
                    rtdb_types::WorldShadow::Present(fact) => validate_owned_schema(schema, fact)?,
                    rtdb_types::WorldShadow::Deleted if create_only => return Err(unavailable()),
                    rtdb_types::WorldShadow::Deleted => (),
                }
                if &shadow.record(&id)? != row { return Err(unavailable()); }
                if create_only {
                    self.require_world_create_absence(owner, &root, relation, &id).await?;
                }
            }
            let bounded_held = schema.requires_bounded_period() && matches!(record.mode, WorldClockMode::Held { .. });
            if self.context.world_constraints.is_none() {
                self.require_noncharging_world_write(owner, &root, schema, documents.rows()).await?;
                if snapshot.contains_table(&root.unique_table(owner))
                    && !documents.rows().iter().all(rtdb_types::WorldShadow::is_tombstone)
                {
                    return Err(Anomaly::incorrect("rtdb.owned/world-unique-uncovered", "World writes require inherited unique occupancy"));
                }
            }
            if bounded_held {
                for row in documents.rows() {
                    let id = crate::source::document_id(row, &self.context.memory)?;
                    self.reserve_bounded_period_write(relation, id)?;
                }
            }
            return self.put_documents(&table, documents, iids, Some(from), Some(to),
                DocumentWrite::Owned { relation, owner, create_only }).await;
        }
        let declaration = self.read_owned_schema(owner, relation).await?;
        let filled = if uses_ordered_micros {
            let (schema, _) = declaration.as_ref().ok_or_else(ordered_micros::invalid)?;
            Some(ordered_micros::fill(
                documents,
                self.tx_key().system_time,
                |column| column.starts_with("p_")
                    && schema.path_column_type(column) == Some(rtdb_types::ScalarType::Binary),
                &self.context.memory,
            )?)
        } else {
            None
        };
        let documents = filled.as_ref().unwrap_or(documents);
        if let Some(source) = self.hosted_replay.as_ref().map(|replay| replay.source_time()) {
            let (schema, _retained) = declaration.as_ref().ok_or_else(replay_period_unavailable)?;
            let valid = replay_period(from, to)?;
            schema.require_replayed_period(valid, source, EventOp::Put)?;
            let lifetime = schema.lifetime_micros().is_some();
            if !documents.has_exact_presence() || (lifetime && !create_only) {
                return Err(replay_period_unavailable());
            }
            let context = if lifetime {
                Some(self.context.query_context(self.snapshot()?, self.tx_key().system_time,
                    &self.options.default_tz, self.trace_parent)?.with_accelerators(Arc::from([])))
            } else { None };
            let mut identities = BTreeSet::new();
            for row in documents.rows() {
                if row.contains_key("_valid_from") || row.contains_key("_valid_to") {
                    return Err(replay_period_unavailable());
                }
                validate_owned_schema(schema, row)?;
                if let Some(context) = &context {
                    let id = crate::source::document_id(row, &self.context.memory)?;
                    if !identities.insert(id.clone())
                        || rtdb_query::owned_purge_floor(Arc::clone(context), owner, relation, &id).await?.is_some()
                        || self.read_owned_revision(owner, relation, &id, Interval::ALL).await?.is_some()
                    { return Err(insert_exists()); }
                }
                if schema.requires_bounded_period() {
                    let id = crate::source::document_id(row, &self.context.memory)?;
                    self.reserve_bounded_period_write(relation, id)?;
                }
            }
            return self.put_documents(&table, documents, iids, Some(valid.lower()), Some(valid.upper()),
                DocumentWrite::Owned { relation, owner, create_only }).await;
        }
        if let Some((schema, _retained)) = declaration {
            schema.require_root_write_period(from, to)?;
            if !documents.has_exact_presence() {
                return Err(Anomaly::incorrect("rtdb.owned/field-presence", "Declared owned writes require exact field presence"));
            }
            for row in documents.rows() {
                let row_bounds = row.get("_valid_from").is_some() || row.get("_valid_to").is_some();
                if schema.requires_bounded_period() {
                    if row_bounds {
                        return Err(Anomaly::incorrect(
                            "rtdb.owned/validity-policy", "A bounded period belongs to the ordered operation",
                        ));
                    }
                    let id = crate::source::document_id(row, &self.context.memory)?;
                    self.reserve_bounded_period_write(relation, id)?;
                } else {
                    schema.require_write_validity(row_bounds)?;
                }
                validate_owned_schema(&schema, row)?;
            }
            if let Some(lifetime) = schema.lifetime_micros() {
                if !create_only { return Err(lifetime_uncovered()); }
                let created = self.tx_key().system_time;
                let expires = Timestamp(created.0.checked_add(lifetime).ok_or_else(overflow)?);
                let context = self.context.query_context(self.snapshot()?, created, &self.options.default_tz, self.trace_parent)?
                    .with_accelerators(Arc::from([]));
                let floor = rtdb_query::read_owned_lifetime_floor(Arc::clone(&context), owner, relation).await?;
                floor.check(lifetime, created)?;
                let mut next = rtdb_types::OwnedLifetimeFloor::at(lifetime, created)?;
                next.swept_present_through = floor.swept_present_through;
                for row in documents.rows() {
                    let id = crate::source::document_id(row, &self.context.memory)?;
                    if rtdb_query::owned_purge_floor(Arc::clone(&context), owner, relation, &id).await?.is_some()
                        || self.read_owned_revision(owner, relation, &id, Interval::ALL).await?.is_some() {
                        return Err(insert_exists());
                    }
                }
                if next.expired_created_through > floor.expired_created_through {
                    self.buffer.put(relation.lifetime_table(owner), next.record(relation), Interval::ALL)?;
                }
                return self.put_documents(&table, documents, iids, Some(created), Some(expires),
                    DocumentWrite::Owned { relation, owner, create_only }).await;
            }
        }
        self.put_documents(&table, documents, iids, from, to, DocumentWrite::Owned { relation, owner, create_only }).await
    }

    async fn write_owned_arrow(
        &mut self, relation: &rtdb_types::OwnedRelation, documents: &ArrowDocuments,
        replay: Option<(&[Iid], &[std::collections::BTreeSet<String>])>,
        from: Option<Timestamp>, to: Option<Timestamp>, create_only: bool,
    ) -> Result<()> {
        if !relation.is_root_data() {
            return Err(rtdb_types::OwnedWorld::unavailable());
        }
        let owner = self.options.user.as_deref().filter(|owner| !owner.is_empty()).ok_or_else(no_owned_principal)?;
        let table = relation.table(owner);
        let declaration = self.read_owned_schema(owner, relation).await?;
        let base_time = self.tx_key().system_time;
        let restoring = self.hosted_replay.as_ref().map(|replay| replay.source_time());
        let mut lifetime_context = None;
        let mut lifetime_period = None;
        let mut lifetime_floor = None;
        if let Some((schema, _)) = &declaration {
            if let Some(source) = restoring {
                let valid = replay_period(from, to)?;
                schema.require_replayed_period(valid, source, EventOp::Put)?;
            } else {
                schema.require_root_write_period(from, to)?;
            }
            if let Some(duration) = schema.lifetime_micros() {
                if !create_only {
                    return Err(if restoring.is_some() { replay_period_unavailable() } else { lifetime_uncovered() });
                }
                let context = self.context.query_context(self.snapshot()?, base_time,
                    &self.options.default_tz, self.trace_parent)?.with_accelerators(Arc::from([]));
                if restoring.is_none() {
                    let end = Timestamp(base_time.0.checked_add(duration).ok_or_else(overflow)?);
                    lifetime_period = Some(interval(base_time, end)?);
                    let floor = rtdb_query::read_owned_lifetime_floor(Arc::clone(&context), owner, relation).await?;
                    floor.check(duration, base_time)?;
                    let mut next = rtdb_types::OwnedLifetimeFloor::at(duration, base_time)?;
                    next.swept_present_through = floor.swept_present_through;
                    if next.expired_created_through > floor.expired_created_through { lifetime_floor = Some(next); }
                }
                lifetime_context = Some(context);
            }
        } else if restoring.is_some() {
            return Err(replay_period_unavailable());
        }
        let mut lifetime_ids = BTreeSet::new();
        let array = documents.array();
        if replay.is_some_and(|(iids, presence)| iids.len() != array.len() || presence.len() != array.len()) {
            return Err(Anomaly::incorrect("xtdb/invalid-iids", "Document and IID counts differ"));
        }
        let memory = Arc::clone(&self.context.memory);
        let _scratch = memory.reserve(4096usize.saturating_add(array.fields().len().saturating_mul(256)))?;
        let row_cells = |row: usize| array.fields().iter().zip(array.columns()).map(move |(field, values)| ArrowCell { field, array: values.as_ref(), row });
        let endpoint = |cell: ArrowCell<'_>| -> Result<Option<Timestamp>> {
            if cell.is_null()? { return Ok(None); }
            cell.zoned_micros().map(Some).ok_or_else(|| Anomaly::fault("xtdb/invalid-timestamp-col-type", "Invalid timestamp col type"))
        };
        for row in 0..array.len() {
            let selected = replay.map(|(_, presence)| &presence[row]);
            let cell_is_present = |cell: ArrowCell<'_>| -> Result<bool> {
                Ok(selected.is_none_or(|selection| selection.contains(cell.field.name())) && !cell.is_undefined()?)
            };
            let id_cell = row_cells(row).find(|cell| cell.field.name() == "_id")
                .ok_or_else(|| Anomaly::incorrect("rtdb.owned/missing-id", "Owned row has no _id"))?;
            let _id = memory.reserve(rtdb_columnar::estimate_copy_cell_bytes(id_cell.field, id_cell.array, row)?.saturating_mul(4))?;
            let id = documents.owned_id(row)?;
            let iid = id.iid();
            if !cell_is_present(id_cell)? || replay.is_some_and(|(iids, _)| iids[row] != iid) {
                return Err(Anomaly::incorrect("xtdb/invalid-iids", "Owned document IID does not match its identity"));
            }
            let mut filled = BTreeMap::<&str, Vec<u8>>::new();
            let mut slot_reservations = Vec::new();
            if let Some(slots) = row_cells(row).find(|cell| cell.field.name() == ordered_micros::FIELD)
                && cell_is_present(slots)?
            {
                let (schema, _) = declaration.as_ref().filter(|_| {
                    self.context.owned_witness_scope.is_some() && restoring.is_none()
                }).ok_or_else(ordered_micros::invalid)?;
                if !slots.is_null()? {
                    let slots = slots.binary().ok_or_else(ordered_micros::invalid)?;
                    ordered_micros::fill_slots(slots, base_time,
                        |column| column.starts_with("p_") && schema.path_column_type(column) == Some(ScalarType::Binary),
                        |name, start, bytes| {
                            if !filled.contains_key(name) {
                                let cell = row_cells(row).find(|cell| cell.field.name() == name).ok_or_else(ordered_micros::invalid)?;
                                if !cell_is_present(cell)? { return Err(ordered_micros::invalid()); }
                                let source = cell.binary().ok_or_else(ordered_micros::invalid)?;
                                let bound = source.len().checked_mul(2).and_then(|size| size.checked_add(512)).ok_or_else(overflow)?;
                                slot_reservations.push(memory.reserve(bound)?);
                                filled.insert(cell.field.name(), source.to_vec());
                            }
                            let cell = filled.get_mut(name).ok_or_else(ordered_micros::invalid)?;
                            ordered_micros::fill_bytes(cell, start, bytes)
                        },
                    )?;
                }
            }
            let mut types = StructFields::new();
            let mut fields = Vec::new();
            let mut columns = Vec::new();
            let mut fields_present = 0;
            let mut explicit_validity = false;
            let mut valid_from = from.unwrap_or(base_time);
            let mut valid_to = to.unwrap_or(Timestamp::MAX);
            for (cell, source) in row_cells(row).zip(array.columns()) {
                if !cell_is_present(cell)? { continue; }
                fields_present += 1;
                match cell.field.name().as_str() {
                    ordered_micros::FIELD => continue,
                    "_valid_from" | "_valid_to" => {
                        explicit_validity = true;
                        if let Some(time) = endpoint(cell)? {
                            if cell.field.name() == "_valid_from" { valid_from = time; } else { valid_to = time; }
                        }
                    }
                    name => {
                        let ty = cell.logical_type()?;
                        rtdb_columnar::validate_readable_cell(cell.field, cell.array, &rtdb_columnar::from_field(cell.field)?, row, &memory)?;
                        types.insert(name.to_owned(), ty);
                        if let Some(bytes) = filled.remove(name) {
                            fields.push(arrow_schema::Field::new(name, DataType::Binary, false));
                            columns.push(Arc::new(arrow_array::BinaryArray::from_iter_values([bytes])) as arrow_array::ArrayRef);
                        } else {
                            fields.push(cell.field.clone());
                            columns.push(source.slice(row, 1));
                        }
                    }
                }
            }
            if selected.is_some_and(|expected| expected.len() != fields_present) {
                return Err(Anomaly::incorrect("rtdb.tx/document-presence", "Document presence names an unknown column"));
            }
            if restoring.is_some() && explicit_validity { return Err(replay_period_unavailable()); }
            if let Some((schema, _)) = &declaration {
                if schema.requires_bounded_period() {
                    if explicit_validity {
                        return Err(Anomaly::incorrect(
                            "rtdb.owned/validity-policy", "A bounded period belongs to the ordered operation",
                        ));
                    }
                    self.reserve_bounded_period_write(relation, id.clone())?;
                } else if restoring.is_none() {
                    schema.require_write_validity(explicit_validity)?;
                }
                schema.validate_types(&types)?;
                for (field, column) in fields.iter().zip(&columns) {
                    if matches!(types.get(field.name()), Some(LogicalType::Scalar(ScalarType::Decimal { .. }))) {
                        rtdb_columnar::validate_projected_decimals(field, column.as_ref())?;
                    }
                }
            }
            if let Some(context) = &lifetime_context {
                if (restoring.is_some() && !lifetime_ids.insert(id.clone()))
                    || rtdb_query::owned_purge_floor(Arc::clone(context), owner, relation, &id).await?.is_some()
                    || self.read_owned_revision(owner, relation, &id, Interval::ALL).await?.is_some()
                { return Err(insert_exists()); }
            }
            let valid_time = match lifetime_period {
                Some(valid) => valid,
                None => interval(valid_from, valid_to)?,
            };
            let key = self.admit_owned_put(&table, relation, owner, &id, iid, valid_time, create_only).await?
                .ok_or_else(|| Anomaly::fault("rtdb.owned/missing-entity-key", "Governed put has no complete key"))?;
            let content = rtdb_columnar::build_struct_array(fields.into(), &columns, 1, None)?;
            let header = EventHeader { commit_position: 0, iid, system_from: base_time, valid_time, op: EventOp::Put };
            let fixed_before = self.fixed_range_before(owner, relation, &id, valid_time).await;
            let fixed = self.fixed_range_pair(&id, fixed_before, |column| {
                let content = content.as_any().downcast_ref::<arrow_array::StructArray>()
                    .ok_or_else(rtdb_types::change_feed_uncovered)?;
                let Some((field, values)) = content.fields().iter().zip(content.columns())
                    .find(|(field, _)| field.name() == column)
                else { return Err(rtdb_types::change_feed_uncovered()); };
                let size = rtdb_columnar::estimate_read_value_bytes(field, values.as_ref(), 0)?;
                let _charge = memory.reserve(size.checked_add(512).ok_or_else(overflow)?)?;
                let value = rtdb_columnar::read_value(field, values.as_ref(), 0)?;
                rtdb_types::FixedRangeKey::from_cells(&value, &id).map(Some)
            });
            let position = self.buffer.append_owned_put(&table, content, header, &key, documents.source_bytes())?;
            self.record_owned_change(relation, id, rtdb_types::OwnedChangeKind::Put, valid_time, position, fixed)?;
        }
        if let Some(floor) = lifetime_floor {
            self.buffer.put(relation.lifetime_table(owner), floor.record(relation), Interval::ALL)?;
        }
        Ok(())
    }

    async fn admit_owned_put(
        &mut self, table: &TableRef, relation: &rtdb_types::OwnedRelation, owner: &str,
        id: &EntityId, iid: Iid, valid: Interval, create_only: bool,
    ) -> Result<Option<Arc<[u8]>>> {
        let key = if relation.is_world_catalog() {
            None
        } else {
            if iid != id.iid() {
                return Err(Anomaly::incorrect("rtdb.owned/invalid-locality", "Owned row locality differs from the canonical key"));
            }
            self.track_unique_mutation(owner, relation, id, valid).await?;
            self.track_capacity_mutation(owner, relation, id, valid).await?;
            Some(id.handle_bytes().into())
        };
        if self.snapshot()?.contains_table(table) {
            let _ = self.read_owned_revision(owner, relation, id, Interval::ALL).await?;
            if create_only && self.read_owned_revision(owner, relation, id, valid).await?.is_some() {
                return Err(insert_exists());
            }
        }
        Ok(key)
    }

    async fn require_absent(&self, table: &TableRef, iid: Iid, valid: Interval) -> Result<()> {
        use rtdb_plan::{Catalog, ColumnId, Expr, Field, QualifiedTable, Relation, Scan, TemporalFilter};

        let snapshot = self.snapshot()?;
        if !snapshot.contains_table(table) { return Ok(()); }
        let context = self.context.query_context(snapshot, self.tx_key().system_time, &self.options.default_tz, self.trace_parent)?
            .with_accelerators(Arc::from([]));
        let table = context.catalog.table(&QualifiedTable { database: self.context.database.clone(), table: table.clone() })?
            .ok_or_else(|| Anomaly::fault("rtdb.tx/insert-table", "Insert table disappeared from its ordered snapshot"))?;
        let field = Field {
            id: ColumnId(0), name: "_iid".into(), qualifier: None, qualified_only: false, unnamed: false,
            data_type: table.scan_column_type("_iid"),
        };
        let scan = Relation::scan(Scan {
            table, columns: vec![field],
            valid_time: TemporalFilter::FromTo { from: Expr::Literal(time_value(valid.lower())), to: Expr::Literal(time_value(valid.upper())) },
            system_time: TemporalFilter::Default, clamp_valid_time: false,
            iids: Some([iid].into()), predicates: Vec::new(), top_k: None, ordered_top_k: None, matches: Vec::new(),
        })?;
        let query = PreparedQuery::new_in(Arc::new(scan), &context.memory)?;
        require_empty_insert(query.execute_columns(context, Vec::new()).await?).await
    }

    async fn put_documents(&mut self, table: &TableRef, documents: &crate::Documents, iids: Option<&[Iid]>, from: Option<Timestamp>, to: Option<Timestamp>, write: DocumentWrite<'_>) -> Result<()> {

        if iids.is_some_and(|iids| iids.len() != documents.rows().len()) { return Err(Anomaly::incorrect("xtdb/invalid-iids", "Document and IID counts differ")); }
        let (relation, owner, create_only) = match write {
            DocumentWrite::Owned { relation, owner, create_only } => (relation, owner, create_only),
            DocumentWrite::Put | DocumentWrite::Insert => {
                let documents = crate::source::owned_row_documents(documents, &self.context.memory)?;
                return self.put_arrow_documents(table, &documents, iids, from, to, matches!(write, DocumentWrite::Insert)).await;
            }
        };
        let is_content = |name: &str| !matches!(name, "_valid_from" | "_valid_to");
        for (index, document) in documents.rows().iter().enumerate() {
            let iid = match iids { Some(iids) => iids[index], None => crate::source::document_id(document, &self.context.memory)?.iid() };
            let default_from = from.unwrap_or(self.tx_key().system_time);
            let default_to = to.unwrap_or(Timestamp::MAX);
            let row_from = document.get("_valid_from").map(|value| timestamp(value, default_from)).transpose()?.unwrap_or(default_from);
            let row_to = document.get("_valid_to").map(|value| timestamp(value, default_to)).transpose()?.unwrap_or(default_to);
            let valid_time = interval(row_from, row_to)?;
            let id = crate::source::document_id(document, &self.context.memory)?;
            let kind = if relation.world().is_some() && rtdb_types::WorldShadow::is_tombstone(document) {
                rtdb_types::OwnedChangeKind::Delete
            } else {
                rtdb_types::OwnedChangeKind::Put
            };
            let entity_key = self.admit_owned_put(table, relation, owner, &id, iid, valid_time, create_only).await?;
            let fixed_before = self.fixed_range_before(owner, relation, &id, valid_time).await;
            let fixed = self.fixed_range_pair(&id, fixed_before, |column| {
                let value = document.get(column).ok_or_else(rtdb_types::change_feed_uncovered)?;
                rtdb_types::FixedRangeKey::from_cells(value, &id).map(Some)
            });
            let document = document.iter().filter(|(name, _)| is_content(name)).map(|(name, value)| (name.clone(), value.clone())).collect();
            let position = self.buffer.append_to(table, StoredEvent { origin: None, commit_position: 0, entity_key, iid, system_from: self.tx_key().system_time, valid_time, operation: StoredOperation::Put(document), recency: None })?;
            if !relation.is_world_catalog() {
                self.record_owned_change(relation, id, kind, valid_time, position, fixed)?;
            }
        }
        if let Some(types) = documents.declared_types().filter(|_| !documents.rows().is_empty()) {
            self.buffer.declare_document_types(table, types.iter().filter(|(name, _)| is_content(name)))?;
        }
        Ok(())
    }
    fn record_owned_change(
        &mut self, relation: &rtdb_types::OwnedRelation, id: rtdb_types::EntityId,
        kind: rtdb_types::OwnedChangeKind, valid: Interval,
        assertion_ordinal: Option<NonZeroU32>,
        fixed: Result<Option<(FixedRangeSide, Arc<MemoryReservation>)>>,
    ) -> Result<()> {
        let too_large = || Anomaly::incorrect("rtdb.owned/change-group-too-large", "An owned relation's change group exceeds its byte limit");
        use rtdb_types::{OwnedChange, OwnedChangeKind};
        let group = self.owned_changes.entry(relation.name().to_owned()).or_insert_with(|| PendingOwnedChanges {
            fixed_sides: Some(Vec::new()), ..PendingOwnedChanges::default()
        });
        match (group.fixed_sides.as_mut(), fixed) {
            (Some(sides), Ok(Some((pair, charge))))
                if sides.len() < rtdb_types::FIXED_RANGE_MAX_IDS
                    && group.fixed_bytes.saturating_add(charge.bytes()) <= fixed_range::MAX_SIDE_BYTES =>
            {
                group.fixed_bytes += charge.bytes();
                sides.push(pair);
                group.reservations.push(charge);
            },
            (_, Ok(None)) | (None, _) => (),
            (Some(_), _) => group.fixed_sides = None,
        }
        if group.erased || matches!(kind, OwnedChangeKind::Erase | OwnedChangeKind::Expired(_)) {
            for previous in &group.touches {
                let previous = OwnedChange::from_value(previous)?;
                if previous.id == id && (matches!(kind, OwnedChangeKind::Erase | OwnedChangeKind::Expired(_)) || matches!(previous.kind, OwnedChangeKind::Erase | OwnedChangeKind::Expired(_))) {
                    return Err(Anomaly::incorrect("rtdb.owned/purge-overlap", "Owned erase cannot share its identity with another mutation in the same commit"));
                }
            }
        }
        let entry = OwnedChange { id, kind, valid, assertion_ordinal }.to_value();
        let bytes = entry.retained_bytes()?.checked_add(128).ok_or_else(too_large)?;
        let group_bytes = group.bytes.checked_add(bytes).filter(|size| *size <= rtdb_types::OWNED_CHANGE_GROUP_BYTES)
            .ok_or_else(too_large)?;
        let charge = self.context.memory.reserve(bytes)?;
        group.touches.push(entry);
        group.reservations.push(charge);
        group.bytes = group_bytes;
        group.erased |= matches!(kind, OwnedChangeKind::Erase | OwnedChangeKind::Expired(_));
        Ok(())
    }

    async fn write_owned_changes(&mut self) -> Result<()> {
        use rtdb_types::OwnedRelation;

        if self.owned_changes.is_empty() { return Ok(()); }
        let owner = self.options.user.as_deref().filter(|owner| !owner.is_empty()).ok_or_else(no_owned_principal)?;
        let base = self.buffer.base_snapshot();
        for (name, group) in std::mem::take(&mut self.owned_changes) {
            self.context.check_cancelled()?;
            let relation = OwnedRelation::from_source(name)?;
            let journal = relation.change_journal(owner)?;
            let frontier = base.relation_event_frontier(&journal)?;
            let previous = base.relation_event_frontier(&relation.table(owner))?;
            let revision = frontier.max(1);
            if frontier == 0 {
                let floor = StructFields::from([
                    ("_id".into(), Value::I64(0)), ("entry".into(), Value::Null),
                    ("through_events".into(), Value::I64(previous)),
                ]);
                self.buffer.put(journal.clone(), floor, Interval::ALL)?;
            } else {
                let context = self.context.query_context(base.clone(), self.tx_key().system_time, &self.options.default_tz, self.trace_parent)?;
                let entry = rtdb_query::read_owned_change_entry(context, owner, &relation, revision - 1).await
                    .map_err(|error| if error.code == "rtdb.owned/change-feed-uncovered" {
                        Anomaly::incorrect("rtdb.owned/change-feed-gap", "The owned relation has a change feed coverage gap")
                    } else { error })?;
                if !matches!(entry.value, Value::List(touches) if !touches.is_empty()) || entry.through_events != previous {
                    return Err(Anomaly::incorrect("rtdb.owned/change-feed-gap", "The owned relation has a change feed coverage gap"));
                }
            }
            let through = previous.checked_add(i64::try_from(group.touches.len()).map_err(|_| overflow())?)
                .ok_or_else(overflow)?;
            if revision == i64::MAX {
                return Err(Anomaly::incorrect("rtdb.owned/change-feed-exhausted", "The owned change feed revision space is exhausted"));
            }
            let rule_table = relation.fixed_range_table(owner);
            let mut entry = StructFields::from([
                ("_id".into(), Value::I64(revision)), ("entry".into(), Value::list(group.touches)),
                ("through_events".into(), Value::I64(through)),
            ]);
            if let Some(sides) = group.fixed_sides
                && (base.contains_table(&rule_table) || self.buffer.has_pending_table(&rule_table))
            {
                let events = through - previous;
                entry.insert("fixed_assertion_keys".into(), Value::Struct(StructFields::from([
                    ("event_count".into(), Value::I64(events)),
                    ("sides".into(), Value::list(sides.into_iter()
                        .map(|side| Value::list(vec![side.before, side.after])).collect())),
                ])));
            }
            self.buffer.put(journal, entry, Interval::ALL)?;
        }
        Ok(())
    }

    async fn put_arrow_documents(&mut self, table: &TableRef, documents: &crate::ArrowDocuments, iids: Option<&[Iid]>, from: Option<Timestamp>, to: Option<Timestamp>, create_only: bool) -> Result<()> {
        let array = documents.array();
        let memory = Arc::clone(&self.context.memory);
        let bytes = array.len().checked_mul(128).and_then(|bytes| bytes.checked_add(4096)).ok_or_else(overflow)?;
        let _scratch = memory.reserve(bytes)?;
        let id = if iids.is_none() { documents.id_reader()? } else { None };
        let endpoint = |name: &str| -> Result<_> {
            array.fields().iter().enumerate()
                .find(|(_, field)| field.name() == name && field.data_type() != &arrow_schema::DataType::Null)
                .map(|(index, field)| OutputColumn::new(field, array.column(index).as_ref()))
                .transpose()
        };
        let row_from = endpoint("_valid_from")?;
        let row_to = endpoint("_valid_to")?;
        let row_time = |column: &Option<OutputColumn<'_>>, row: usize, default| {
            match column { Some(column) => column.time(row, default), None => Ok(default) }
        };
        let system_from = self.tx_key().system_time;
        let header = |row| {
            let iid = match iids {
                Some(iids) => iids.get(row).copied().ok_or_else(|| Anomaly::incorrect("xtdb/invalid-iids", "Document and IID counts differ"))?,
                None => documents.iid(row, id.as_ref(), &memory)?,
            };
            let valid_time = interval(row_time(&row_from, row, from.unwrap_or(system_from))?, row_time(&row_to, row, to.unwrap_or(Timestamp::MAX))?)?;
            Ok(EventHeader { commit_position: 0, iid, system_from, valid_time, op: EventOp::Put })
        };
        let (fields, columns): (Vec<_>, Vec<_>) = array.fields().iter().zip(array.columns())
            .filter(|(field, _)| !matches!(field.name().as_str(), "_valid_from" | "_valid_to"))
            .map(|(field, column)| (Arc::clone(field), Arc::clone(column))).unzip();
        let content = rtdb_columnar::build_struct_array(fields.into(), &columns, array.len(), None)?;
        if create_only {
            for row in 0..array.len() {
                let event = header(row)?;
                self.require_absent(table, event.iid, event.valid_time).await?;
                let batch = rtdb_format::data::owned_put_batch(content.slice(row, 1), &[event], documents.source_bytes(), &memory)?;
                self.buffer.append_batch(table, batch)?;
            }
            return Ok(());
        }
        let headers = (0..array.len()).map(header).collect::<Result<Vec<_>>>()?;
        let batch = rtdb_format::data::owned_put_batch(content, &headers, documents.source_bytes(), &memory)?;
        self.buffer.append_batch(table, batch)
    }
    async fn check_replay_marker(&self, replay: &ReplaySubmission, observed: Timestamp) -> Result<()> {
        if observed >= replay.not_after() {
            return Err(Anomaly::incorrect("rtdb.node/replay-window-elapsed", "Replay submission reached its exclusive cutoff in the ordered log"));
        }
        if !self.snapshot()?.contains_table(&replay_table()) { return Ok(()); }
        let mut cursor = self.open_query(
            "SELECT original_tx_id, _system_from FROM xt.rtdb_replay_keys FOR VALID_TIME FROM ? TO NULL WHERE _id = ? ORDER BY _system_from DESC LIMIT 1",
            vec![time_value(replay.submitted_at()), Value::Utf8(replay.key().to_owned())],
        ).await?;
        while let Some(batch) = cursor.next_batch().await? {
            let batch = batch.batch();
            if batch.num_rows() == 0 { continue; }
            let id = rtdb_columnar::read_value(batch.schema().field(0), batch.column(0).as_ref(), 0)?;
            let system = rtdb_columnar::read_value(batch.schema().field(1), batch.column(1).as_ref(), 0)?;
            let (Value::I64(id), Value::Timestamp { value, unit: TimeUnit::Microsecond, .. }) = (id, system) else {
                return Err(Anomaly::fault("rtdb.tx/private-replay-marker", "Replay marker has no original transaction key"));
            };
            return Err(duplicate_replay(TransactionKey { tx_id: TxId(id), system_time: Timestamp(value) }));
        }
        Ok(())
    }

    fn write_replay_marker(&mut self, replay: &ReplaySubmission) -> Result<()> {
        let document = StructFields::from([
            ("_id".into(), Value::Utf8(replay.key().to_owned())),
            ("original_tx_id".into(), Value::I64(self.tx_key().tx_id.0)),
        ]);
        self.buffer.put(replay_table(), document, interval(replay.submitted_at(), replay.expires_at())?)
    }

    fn write_result(&mut self, result: &TransactionResult, metadata: Option<&UserMetadata>, appended: Timestamp, observed: Timestamp) -> Result<()> {
        let key = result.tx_key();
        let error = result.encoded_error()?;
        let bound = error.as_ref().map_or(0, |error| error.as_slice().len()).checked_add(metadata.map(|value| value.value().retained_bytes()).transpose()?.unwrap_or(0)).and_then(|bytes| bytes.checked_add(16_384)).ok_or_else(overflow)?;
        let _reservation = self.context.memory.reserve(bound)?;
        let table = TableRef::new("xt", "txs");
        let error_column = "error".to_owned();
        let metadata_column = "user_metadata".to_owned();
        let metadata_type = metadata.map_or_else(
            || LogicalType::Struct(StructFields::new()).maybe(),
            |metadata| metadata.value().logical_type().maybe(),
        );
        if let Some(metadata) = metadata {
            let expiry = Timestamp(observed.0.saturating_add(RECEIPT_WINDOW_MICROS));
            let retained = StructFields::from([
                ("_id".into(), Value::I64(key.tx_id.0)),
                (metadata_column.clone(), metadata.value().clone()),
            ]);
            self.buffer.put(receipts_table(), retained, interval(observed, expiry)?)?;
        }
        let document = StructFields::from([
            ("_id".into(), Value::I64(key.tx_id.0)), ("system_time".into(), time_value(key.system_time)),
            ("source_log_time".into(), time_value(appended)),
            ("receipt_time".into(), time_value(observed)),
            ("committed".into(), Value::Bool(result.committed())),
            (metadata_column.clone(), Value::Null),
            (error_column.clone(), error.map(|error| Value::Transit(error.as_slice().to_vec())).unwrap_or(Value::Null)),
        ]);
        self.buffer.put(table.clone(), document, interval(key.system_time, Timestamp::MAX)?)?;
        self.buffer.declare_document_types(&table, [(&error_column, &LogicalType::Scalar(ScalarType::Transit).maybe()), (&metadata_column, &metadata_type)])
    }
}

pub const HISTORICAL_KEYED_TRANSACTION_SQL: &str = "SELECT _id, system_time FROM xt.txs FOR ALL VALID_TIME WHERE committed = true AND system_time = (SELECT k._system_from FROM public.rtdb_idempotency_keys FOR SYSTEM_TIME AS OF ? FOR VALID_TIME FROM ? TO NULL AS k WHERE k._id = ? ORDER BY k._system_from DESC LIMIT 1)";

/// How long, from source-log admission, a receipt answers retries of its identity.
pub const RECEIPT_WINDOW_MICROS: i64 = 3_600_000_000;
fn receipt_bucket(time: Timestamp) -> i64 { time.0.div_euclid(RECEIPT_WINDOW_MICROS) }
fn receipt_id(id: &str, bucket: i64) -> Value { Value::Utf8(format!("{bucket}/{id}")) }

fn receipts_table() -> TableRef { TableRef::new("xt", "idempotency_receipts") }

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

async fn execute(input: Arc<rtdb_plan::Relation>, context: Arc<QueryContext>, arguments: Option<BoundArgumentRow<'_>>, syntax: Option<Arc<MemoryReservation>>) -> Result<QueryCursor> {
    let count = input.parameter_count();
    if arguments.is_none() && count != 0 { return Err(Anomaly::incorrect("xtdb.indexer/missing-sql-args", "Arguments list was expected but not provided").with_context("param-count", count.into())); }
    let width = arguments.map_or(0, BoundArgumentRow::width);
    if width != count { return Err(Anomaly::incorrect("xtdb.indexer/incorrect-sql-arg-count", format!("Parameter error: {width} provided, {count} expected")).with_context("param-count", count.into()).with_context("arg-count", width.into())); }
    let query = PreparedQuery::new_in(input, &context.memory)?;
    drop(syntax);
    match arguments {
        Some(BoundArgumentRow::Arrow { batch, row, types }) => query.execute_arrow_columns(context, batch.clone(), row, types, None).await,
        Some(BoundArgumentRow::Values { values, .. }) => query.execute_columns(context, values.to_vec()).await,
        None => query.execute_columns(context, Vec::new()).await,
    }
}
async fn require_empty_insert(mut cursor: QueryCursor) -> Result<()> {
    while let Some(batch) = cursor.next_batch().await? {
        if batch.batch().num_rows() != 0 {
            return Err(insert_exists());
        }
    }
    Ok(())
}

fn insert_exists() -> Anomaly {
    Anomaly::new(AnomalyCategory::Conflict, "rtdb.tx/exists", "Insert overlaps an existing document")
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum OwnedPathKind {
    Ordered,
    Search,
    Spatial(rtdb_types::SpatialGeometryEncoding),
}

fn invalid_search_path_column() -> Anomaly {
    Anomaly::incorrect("rtdb.owned/search-column", "Owned search requires one stored clear-text UTF-8 column")
}

fn replay_period(from: Option<Timestamp>, to: Option<Timestamp>) -> Result<Interval> {
    let (Some(from), Some(to)) = (from, to) else { return Err(replay_period_unavailable()); };
    interval(from, to)
}

fn replay_period_unavailable() -> Anomaly {
    Anomaly::incorrect("rtdb.owned/restore-validity", "Native replay requires explicit original intervals and an exact declared effect")
}

fn validate_owned_schema(schema: &rtdb_types::OwnedSchema, row: &StructFields<Value>) -> Result<()> {
    schema.validate_row(row)?;
    for value in row.values().filter(|value| matches!(value, Value::Decimal(_))) {
        rtdb_columnar::build_array(&value.logical_type(), std::slice::from_ref(value))?;
    }
    Ok(())
}

fn purge_path_uncovered() -> Anomaly {
    Anomaly::incorrect("rtdb.owned/purge-path-uncovered", "Owned erase is unsupported while the relation has a search or final-state constraint path")
}

fn relation_retired() -> Anomaly {
    Anomaly::new(AnomalyCategory::Conflict, "rtdb.owned/relation-retired", "The owned relation is retired and accepts no write or declaration")
}

fn no_owned_principal() -> Anomaly {
    Anomaly::new(AnomalyCategory::Forbidden, "rtdb.owned/no-owner", "Owned relations require an authenticated principal")
}

fn validate_document_operation(operation: &Operation, owner: Option<&str>) -> Result<()> {
    let put_columns = &["_id", "_fn", "_valid_from", "_valid_to"][..];
    let root_columns = &["_id", "_fn", "_valid_from", "_valid_to", ordered_micros::FIELD][..];
    if let Operation::Public(TxOp::OwnedArrowDocs { relation, documents, .. }) | Operation::OwnedArrowIids { relation, documents, .. } = operation {
        let owner = owner.filter(|owner| !owner.is_empty()).ok_or_else(no_owned_principal)?;
        let permitted = if relation.is_root_data() { root_columns } else { put_columns };
        return validate_document_columns(&relation.table(owner), documents.columns(), "insert", permitted);
    }
    if let Operation::Public(TxOp::OwnedInsertDocs { relation, documents, .. } | TxOp::OwnedReplaceDocs { relation, documents, .. }) | Operation::OwnedWriteIids { relation, documents, .. } = operation {
        let owner = owner.filter(|owner| !owner.is_empty()).ok_or_else(no_owned_principal)?;
        let permitted = if relation.world().is_some() {
            &["_id", rtdb_types::WorldShadow::FIELDS][..]
        } else if relation.is_root_data() {
            root_columns
        } else {
            put_columns
        };
        return validate_document_columns(&relation.table(owner), documents.columns(), "insert", permitted);
    }
    let (table, columns, verb, permitted) = match operation {
        Operation::Public(TxOp::InsertDocs { table, documents, .. }) | Operation::InsertIids { table, documents, .. } =>
            (table, documents.columns(), "insert", put_columns),
        Operation::InsertArrowIids { table, documents, .. } =>
            (table, documents.columns(), "insert", put_columns),
        Operation::Public(TxOp::PutDocs { table, documents, .. }) | Operation::PutIids { table, documents, .. } =>
            (table, documents.columns(), "put", put_columns),
        Operation::Public(TxOp::PutArrowDocs { table, documents }) | Operation::PutReplayArrow { table, documents, .. } =>
            (table, documents.columns(), "put", put_columns),
        Operation::Public(TxOp::PatchDocs { table, documents, .. }) | Operation::PatchIids { table, documents, .. } =>
            (table, documents.columns(), "patch", &["_id", "_fn"][..]),
        Operation::PatchArrowIids { table, documents, .. } =>
            (table, documents.columns(), "patch", &["_id", "_fn"][..]),
        _ => return Ok(()),
    };
    check_table(table)?;
    validate_document_columns(table, columns, verb, permitted)
}

fn validate_document_columns(table: &TableRef, columns: &[String], verb: &str, permitted: &[&str]) -> Result<()> {
    let forbidden: Vec<_> = columns.iter().map(String::as_str)
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
        Operation::Public(TxOp::InsertDocs { table, .. } | TxOp::PutDocs { table, .. } | TxOp::PutArrowDocs { table, .. } | TxOp::DeleteDocs { table, .. } | TxOp::EraseDocs { table, .. })
        | Operation::InsertIids { table, .. } | Operation::InsertArrowIids { table, .. } | Operation::PutIids { table, .. } | Operation::PutReplayArrow { table, .. } => Some(table),
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
fn lifetime_uncovered() -> Anomaly {
    Anomaly::incorrect("rtdb.owned/lifetime-uncovered", "Acceptance-lifetime relations admit only immutable creation and present point reads")
}

#[derive(Clone, Copy)]
enum DocumentWrite<'a> { Insert, Put, Owned { relation: &'a rtdb_types::OwnedRelation, owner: &'a str, create_only: bool } }

#[derive(Clone, Copy)]
enum MutationKind { Put, Delete, Erase }

fn mutation_put_batch(batch: &RecordBatch, system_time: Timestamp, memory: &Arc<MemoryBudget>) -> Result<Option<OwnedBatch>> {
    let Some((index, _)) = batch.schema_ref().column_with_name("doc") else { return Ok(None); };
    let Some(content) = batch.column(index).as_struct_opt() else { return Ok(None); };
    if content.is_empty() || content.null_count() != 0 { return Ok(None); }
    let scalar = content.fields().iter().all(|field| {
        plain_scalar_field(field) && !matches!(field.data_type(), DataType::UInt8 | DataType::UInt16 | DataType::UInt32 | DataType::UInt64)
    });
    // A nullable field stores the scalar leg before null. When a later write adds a different type,
    // a null seen before the first scalar must still take its position in the transaction's union.
    if scalar && content.columns().iter().any(|array| array.is_null(0) && array.null_count() != array.len()) {
        return Ok(None);
    }

    let iid = mutation_column(batch, "_iid")?;
    let from = mutation_column(batch, "_valid_from")?;
    let to = mutation_column(batch, "_valid_to")?;
    let bound = content.fields().iter().try_fold(4096usize, |bytes, field| {
        bytes.checked_add(field.size().checked_mul(4).ok_or_else(overflow)?).ok_or_else(overflow)
    })?.checked_add(rtdb_index::IndexBatch::header_bytes(batch.num_rows())?).ok_or_else(overflow)?;
    let _scratch = memory.reserve(bound)?;
    let read_header = |row| -> Result<EventHeader> {
        Ok(EventHeader {
            commit_position: 0,
            iid: iid.iid(row)?, system_from: system_time,
            valid_time: interval(from.time(row, system_time)?, to.time(row, Timestamp::MAX)?)?, op: EventOp::Put,
        })
    };
    let mut headers = Vec::with_capacity(batch.num_rows());
    for row in 0..batch.num_rows() {
        // The row path orders header errors against admission of earlier documents.
        let Ok(header) = read_header(row) else { return Ok(None); };
        headers.push(header);
    }
    if !scalar {
        let content = RecordBatch::from(content.clone());
        let Some(documents) = ArrowDocuments::from_canonical_ingest(&content, IngestMode::Mutation, memory)? else { return Ok(None); };
        return rtdb_format::data::owned_put_batch(Arc::new(documents.array().clone()), &headers, None, memory).map(Some);
    }
    let copy_bound = content.columns().iter().try_fold(
        content.len().checked_mul(64).and_then(|bytes| bytes.checked_add(4096)).ok_or_else(overflow)?,
        |bytes, array| bytes.checked_add(array.get_array_memory_size().checked_mul(4).ok_or_else(overflow)?).ok_or_else(overflow),
    )?;
    let _copy = memory.reserve(copy_bound)?;
    let cells = (0..content.len()).map(|row| Some((0, row))).collect::<Vec<_>>();
    let columns = content.columns().iter().map(|array| {
        rtdb_columnar::copy_nullable_cells(&[array.as_ref()], &cells)
    }).collect::<Result<Vec<_>>>()?;
    let schema = Arc::new(Schema::new(content.fields().clone()));
    let content = RecordBatch::try_new(schema, columns).map_err(|error| Anomaly::fault("rtdb.tx/mutation-schema", error.to_string()))?;
    let Some(documents) = ArrowDocuments::from_record_batch(&content, memory)? else { return Ok(None); };
    rtdb_format::data::owned_put_batch(Arc::new(documents.array().clone()), &headers, None, memory).map(Some)
}

fn mutation_document(array: &dyn Array, row: usize) -> Option<(&arrow_array::StructArray, usize)> {
    if let Some(document) = array.as_struct_opt() {
        return document.is_valid(row).then_some((document, row));
    }
    let union = array.as_any().downcast_ref::<arrow_array::UnionArray>()?;
    mutation_document(union.child(union.type_id(row)).as_ref(), union.value_offset(row))
}

fn mutation_column<'a>(batch: &'a RecordBatch, name: &str) -> Result<OutputColumn<'a>> {
    let schema = batch.schema_ref();
    let index = schema.index_of(name).map_err(|error| Anomaly::fault("rtdb.tx/mutation-schema", error.to_string()))?;
    OutputColumn::new(schema.field(index), batch.column(index).as_ref())
}

/// The events one batch of a mutation's output writes, in its row order.
fn mutation_events(batch: &RecordBatch, kind: MutationKind, system_time: Timestamp, mut append: impl FnMut(StoredEvent) -> Result<()>) -> Result<()> {
    let column = |name| mutation_column(batch, name);
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
        append(StoredEvent { origin: None, commit_position: 0, entity_key: None, iid, system_from: system_time, valid_time, operation, recency: None })?;
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

    struct CapturingWriter {
        store: Arc<rtdb_storage::LocalStore>,
        proposed_floor: rtdb_types::CatalogCut,
        cursor: Option<QueryCursor>,
    }

    #[async_trait]
    impl TransactionWriter for CapturingWriter {
        async fn write(&mut self, tx: &mut OpenTransaction<'_>) -> Result<WriterResult> {
            use rtdb_storage::ObjectStore;

            assert!(!self.store.advance_catalog_floor(self.proposed_floor).await?,
                "an active transaction must retain its durable catalog");
            tx.execute_sql("INSERT INTO docs RECORDS {_id: 42}", None).await?;
            self.cursor = Some(tx.open_query("SELECT _id FROM docs", Vec::new()).await?);
            Ok(WriterResult::Committed { user_metadata: None })
        }
    }

    #[tokio::test]
    async fn a_transaction_cursor_retains_its_catalog_until_dropped() {
        use rtdb_storage::{LocalStore, ObjectStore};
        use rtdb_types::{CatalogCut, ReplicaPosition, StorageEpoch, StorageVersion};

        let directory = tempfile::tempdir().unwrap();
        let open = || Arc::new(LocalStore::open(directory.path()).unwrap().with_catalog_ledger(StorageEpoch(0)));
        let store = open();
        let mut writer = CapturingWriter {
            store: open(),
            proposed_floor: CatalogCut {
                storage_version: StorageVersion(6),
                storage_epoch: StorageEpoch(0),
                replica_epoch: StorageEpoch(0),
                through: ReplicaPosition(1),
            },
            cursor: None,
        };
        let state = rtdb_index::PartitionState::empty(Default::default()).unwrap();
        state.publish_catalog_cut(CatalogCut { through: ReplicaPosition(0), ..writer.proposed_floor }).unwrap();
        let memory = MemoryBudget::new(1 << 24);
        let context = ResolveContext {
            write_event_origins: false,
            placed_receipt_retirement: false,
            owned_witness_scope: None,
            world_constraints: None,
            database: "xtdb".into(),
            storage: Arc::new(BufferPool::new(store, 1 << 20)),
            memory: Arc::clone(&memory),
            query_options: QueryOptions::default(),
            scan_accelerators: Default::default(),
            cancellation: CancellationToken::new(),
            default_tz: "UTC".into(),
            authorization: Arc::new(NoRoleChecks),
            execution_observer: None,
            tracer: None,
        };

        let mut resolver = TxResolver::new(None, SourcePosition(0), None, &memory).unwrap();
        let outcome = resolver.resolve_external(state.snapshot(), Timestamp(100), None, None, &mut writer, &context).await.unwrap();
        assert!(outcome.result().committed());
        assert!(!writer.store.advance_catalog_floor(writer.proposed_floor).await.unwrap(),
            "the transaction cursor must retain its catalog after resolution");
        let mut cursor = writer.cursor.take().unwrap();
        assert_eq!(cursor.next_batch().await.unwrap().unwrap().batch().num_rows(), 1);
        assert!(cursor.next_batch().await.unwrap().is_none());
        drop(cursor);
        assert!(writer.store.advance_catalog_floor(writer.proposed_floor).await.unwrap());

        let request = SourceTransaction::new(Vec::new(), SubmitOptions::default(), &memory).unwrap();
        let error = resolver.resolve_source(state.snapshot(), SourcePosition(1), Timestamp(101), request, &context).await
            .err().expect("source resolution must check catalog admission");
        assert_eq!(error.code, "rtdb.storage/catalog-not-current");
    }

    #[tokio::test]
    async fn mutation_syntax_is_charged_until_the_plan_takes_ownership() {
        let memory = MemoryBudget::new(1 << 24);
        let resolve = ResolveContext {
            write_event_origins: false,
            placed_receipt_retirement: false,
            owned_witness_scope: None,
            world_constraints: None,
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

    #[tokio::test]
    async fn hosted_capacity_sql_stages_under_the_exact_installed_writer() -> Result<()> {
        struct HostedWriter;
        impl TransactionAuthorization for HostedWriter {
            fn check_role_membership(&self, _: &str, _: Option<&str>) -> Result<()> { panic!("unexpected role check") }
            fn is_hosted_control_owner(&self, principal: &str) -> bool { principal == "owner" }
        }

        let memory = MemoryBudget::new(1 << 24);
        let context = ResolveContext {
            write_event_origins: false,
            placed_receipt_retirement: false,
            owned_witness_scope: Some(rtdb_types::PlacedAttemptScope::new("native-capacity", 1)?),
            world_constraints: None,
            database: "application".into(),
            storage: Arc::new(BufferPool::new(Arc::new(rtdb_storage::MemoryStore::default()), 1 << 20)),
            memory: Arc::clone(&memory),
            query_options: QueryOptions::default(),
            scan_accelerators: Default::default(),
            cancellation: CancellationToken::new(),
            default_tz: "UTC".into(),
            authorization: Arc::new(HostedWriter),
            execution_observer: None,
            tracer: None,
        };
        let state = rtdb_index::PartitionState::empty(Default::default())?;
        let options = SubmitOptions { user: Some("owner".into()), ..Default::default() };
        let mut installation = OpenTransaction::new(
            state.value().snapshot(), TransactionKey { tx_id: TxId(1), system_time: Timestamp(100) }, &options, &context,
        )?;
        installation.buffer.put(TableRef::new("public", "bj_installation"), StructFields::from([
            ("_id".into(), Value::Utf8("authority".into())),
            ("writer".into(), Value::Utf8("installed".into())),
        ]), Interval::new(Timestamp(0), Timestamp::MAX)?)?;
        let mut publication = OpenTransaction::new(
            installation.snapshot()?, TransactionKey { tx_id: TxId(2), system_time: Timestamp(200) }, &options, &context,
        )?;
        let sql = r#"INSERT INTO xt.hosted_capacity_declarations
            (_id, relation, "column", cap, predicate, writer_micros) VALUES (?, ?, ?, ?, ?, ?)"#;
        let mut arguments = [
            Value::Utf8("partition".into()), Value::Utf8("facts".into()), Value::Utf8("partition".into()),
            Value::I64(3), Value::Binary(Vec::new()), Value::I64(99),
        ];
        let error = publication.execute_sql(sql, Some(&arguments)).await.unwrap_err();
        assert_eq!(error.code, "rtdb.hosted/stale-control");
        arguments[5] = Value::I64(100);
        publication.execute_sql(sql, Some(&arguments)).await?;
        publication.finish_owned_capacity().await?;
        let relation = rtdb_types::OwnedRelation::new("facts")?;
        let applied = publication.snapshot()?;
        assert!(applied.contains_table(&relation.capacity_table("owner")));
        assert!(!applied.contains_table(&TableRef::new("xt", HOSTED_CAPACITY_DECLARATIONS)));
        Ok(())
    }

    #[tokio::test]
    async fn a_hosted_root_can_publish_only_a_ready_spatial_index() -> Result<()> {
        use rtdb_types::{EntityId, OwnedRelation, OwnedSchema, PlacedAttemptScope, SpatialGeometryEncoding};

        let scope = PlacedAttemptScope::new("spatial-catalog", 1)?;
        let memory = MemoryBudget::new(1 << 24);
        let context = ResolveContext {
            write_event_origins: false,
            placed_receipt_retirement: false,
            owned_witness_scope: Some(scope),
            world_constraints: None,
            database: "application".into(),
            storage: Arc::new(BufferPool::new(Arc::new(rtdb_storage::MemoryStore::default()), 1 << 20)),
            memory: Arc::clone(&memory),
            query_options: QueryOptions::default(),
            scan_accelerators: Default::default(),
            cancellation: CancellationToken::new(),
            default_tz: "UTC".into(),
            authorization: Arc::new(NoRoleChecks),
            execution_observer: None,
            tracer: None,
        };
        let options = SubmitOptions { user: Some("owner".into()), ..Default::default() };
        let state = rtdb_index::PartitionState::empty(Default::default())?;
        let installed_at = TransactionKey { tx_id: TxId(1), system_time: Timestamp(100) };
        let mut installation = OpenTransaction::new(state.snapshot(), installed_at, &options, &context)?;
        let relation = OwnedRelation::new("places")?;
        let path = OwnedRelation::new("by-location")?;
        let catalog = OwnedRelation::new("bj_fact_root_owner")?;
        let marker = OwnedRelation::new("fact_publications")?;
        let catalog_row = |schema| StructFields::from([
            ("_id".into(), Value::Utf8("root".into())),
            ("schema".into(), Value::I64(schema)),
        ]);
        installation.buffer.put(
            TableRef::new("public", catalog.name()),
            catalog_row(1),
            Interval::new(Timestamp(0), Timestamp::MAX)?,
        )?;
        let schema = OwnedSchema::from_json(
            br#"{"fields":[{"name":"location","type":{"kind":"binary"},"optional":false,"nullable":false}]}"#,
        )?;
        installation.apply(&Operation::Public(TxOp::OwnedDeclareRelation {
            relation: relation.clone(), schema,
        })).await?;
        installation.apply(&Operation::Public(TxOp::OwnedDeclareRelation {
            relation: marker.clone(), schema: OwnedSchema::from_json(br#"{"fields":[]}"#)?,
        })).await?;
        let durable = installation.snapshot()?;
        let mut resolver = TxResolver::new(durable.tx_basis(), SourcePosition(1), None, &memory)?;
        let encoding = SpatialGeometryEncoding::BijectionGeometry;
        let columns = vec!["location".into()];
        let hook_id = EntityId::String("hook".into());
        let hook = RecordBatch::try_from_iter([
            ("_id", Arc::new(arrow_array::StringArray::from(vec!["hook"])) as arrow_array::ArrayRef),
            ("location", Arc::new(arrow_array::BinaryArray::from(vec![b"\x0dcoordinates\0\x01\x0c\x05\x80\0\0\0\0\0\0\0\x05\x80\0\0\0\0\0\0\0\0\0type\0\x01\x09Point\0\x01\0\0".as_slice()]))),
        ]).unwrap();
        let hook = rtdb_columnar::OwnedBatch::new(hook, &memory)?;
        let hook = crate::ArrowDocuments::from_batch(&hook, &memory)?.unwrap();
        let publication = |encoding| -> Result<SourceTransaction> {
            let control = crate::HostedControlWrite::new(
                catalog.clone(), scope, Some(installed_at.system_time), catalog_row(2),
            )?;
            let documents = crate::Documents::new(vec![StructFields::from([
                ("_id".into(), Value::Utf8("head".into())),
            ])], &memory)?;
            let operations = vec![
                TxOp::OwnedDeadline { until: Timestamp(1000), recheck_at_staging: false, journal_before: None },
                TxOp::OwnedReplaceDocs { relation: marker.clone(), documents, valid_from: Some(Timestamp::MIN), valid_to: None },
                TxOp::OwnedArrowDocs { relation: relation.clone(), documents: hook.clone(), create_only: true, valid_from: None, valid_to: None },
                TxOp::OwnedRequireSpatialPath { relation: relation.clone(), name: path.clone(), columns: columns.clone(), encoding },
                TxOp::HostedControl(control),
            ];
            Ok(SourceTransaction::new(operations, options.clone(), &memory)?.with_placed_seed(Some(scope.issue())))
        };

        let before_backfill = resolver.resolve_source(
            durable.clone(), SourcePosition(2), Timestamp(200), publication(encoding)?, &context,
        ).await?;
        assert_eq!(before_backfill.result().error().unwrap().code, "rtdb.owned/spatial-not-ready");

        let declaration = SourceTransaction::new(vec![TxOp::OwnedDeclareEncodedSpatialPath {
            relation: relation.clone(), name: path.clone(), columns: columns.clone(), encoding,
        }], options.clone(), &memory)?.with_placed_seed(Some(scope.issue()));
        let staged = resolver.resolve_source(durable.clone(), SourcePosition(3), Timestamp(300), declaration, &context).await?;
        assert!(staged.result().committed(), "{:?}", staged.result().error());

        let wrong_encoding = resolver.resolve_source(
            durable.clone(), SourcePosition(4), Timestamp(400),
            publication(SpatialGeometryEncoding::TaggedWkb)?, &context,
        ).await?;
        assert_eq!(wrong_encoding.result().error().unwrap().code, "rtdb.owned/spatial-not-ready");

        let ordered = resolver.resolve_source(
            durable.clone(), SourcePosition(5), Timestamp(500), publication(encoding)?, &context,
        ).await?;
        assert!(ordered.result().committed(), "{:?}", ordered.result().error());
        let published = context.query_context(resolver.resolving_snapshot(durable)?, Timestamp(500), "UTC", None)?;
        let observed = rtdb_query::read_hosted_control_point(
            Arc::clone(&published), TableRef::new("public", catalog.name()), &EntityId::String("root".into()), false,
        ).await?;
        let hook_revision = rtdb_query::read_owned_point_revision(
            published, "owner", &relation, &hook_id, Interval::new(Timestamp(500), Timestamp(501))?,
        ).await?;
        assert_eq!(observed.row.map(|(_, revision)| revision), Some(ordered.result().tx_key().system_time));
        assert_eq!(hook_revision, Some(ordered.result().tx_key().system_time));
        Ok(())
    }

    #[tokio::test]
    async fn governed_worlds_order_withdrawals_and_uncapped_replacements() -> Result<()> {
        use rtdb_types::{EntityId, OwnedRelation, OwnedSchema, OwnedWorld, WorldClockMode, WorldSchemas, WorldShadow};

        let memory = MemoryBudget::new(1 << 24);
        let context = ResolveContext {
            write_event_origins: false,
            placed_receipt_retirement: false,
            owned_witness_scope: None,
            world_constraints: None,
            database: "application".into(),
            storage: Arc::new(BufferPool::new(Arc::new(rtdb_storage::MemoryStore::default()), 1 << 20)),
            memory: Arc::clone(&memory),
            query_options: QueryOptions::default(),
            scan_accelerators: Default::default(),
            cancellation: CancellationToken::new(),
            default_tz: "UTC".into(),
            authorization: Arc::new(NoRoleChecks),
            execution_observer: None,
            tracer: None,
        };
        let state = rtdb_index::PartitionState::empty(Default::default())?;
        let options = SubmitOptions { user: Some("owner".into()), ..Default::default() };
        let cut = TransactionKey { tx_id: TxId(1), system_time: Timestamp(100) };
        let relation = OwnedRelation::new("facts")?;
        let mut declaration = OpenTransaction::new(state.value().snapshot(), cut, &options, &context)?;
        let predicate = b"\x09partition\x00\x01x";
        declaration.declare_owned_capacity(&relation, "partition", 0, 3, predicate).await?;
        declaration.finish_owned_capacity().await?;
        let base = declaration.snapshot()?;
        let schema = OwnedSchema::from_json(br#"{"fields":[{"name":"partition","type":{"kind":"binary"},"optional":false,"nullable":false}],"validity":"acceptance"}"#)?;
        let id = EntityId::String("a".into());
        let replace = |key| WorldShadow::Present(StructFields::from([
            ("_id".into(), id.as_value()), ("partition".into(), Value::Binary(vec![key])),
        ])).record(&id);
        let allowed = crate::Documents::new(vec![replace(b'y')?], &memory)?;
        let charged = crate::Documents::new(vec![replace(b'x')?], &memory)?;
        let withdrawn = crate::Documents::new(vec![WorldShadow::Deleted.record(&id)?], &memory)?;
        let ordered = TransactionKey { tx_id: TxId(2), system_time: Timestamp(200) };
        for (mode, valid) in [(WorldClockMode::Proposal, Timestamp(200)), (WorldClockMode::Held { start: Timestamp(100) }, Timestamp(101))] {
            let world = Iid([1; 16]);
            let record = OwnedWorld {
                ancestor: None,
                fork_lineage: Vec::new(),
                id: world, pin: Iid([2; 16]), capture: Iid([3; 16]), suffix: None,
                manifest: "a".repeat(43), basis: cut, parent_publication: None,
                mode, until: Timestamp(1000), dropped: false,
                schemas: WorldSchemas::new(BTreeMap::from([(relation.name().into(), schema.clone())]))?,
            };
            let mut tx = OpenTransaction::new(base.clone(), ordered, &options, &context)?;
            tx.owned_worlds.insert(world, (record, memory.reserve(4096)?));
            let overlay = relation.in_world(world)?;
            let error = tx.write_owned(&overlay, &charged, None, None, None, false).await.unwrap_err();
            assert_eq!(error.code, "rtdb.owned/world-capacity-uncovered");
            if matches!(mode, WorldClockMode::Held { .. }) {
                let error = tx.write_owned(&overlay, &allowed, None, Some(cut.system_time), Some(Timestamp::MAX), false).await.unwrap_err();
                assert_eq!(error.code, "rtdb.owned/validity-policy");
            }
            tx.write_owned(&overlay, &allowed, None, None, None, false).await?;
            assert_eq!(tx.read_owned_revision("owner", &overlay, &id, Interval::at(valid)?).await?, Some(ordered.system_time));
            tx.write_owned(&overlay, &withdrawn, None, None, None, false).await?;
            if matches!(mode, WorldClockMode::Held { .. }) {
                let period_schema = OwnedSchema::from_json(br#"{"fields":[{"name":"partition","type":{"kind":"binary"},"optional":false,"nullable":false}],"validity":"bounded_period"}"#)?;
                let (world, _) = tx.owned_worlds.get_mut(&world).expect("original test world");
                world.schemas = WorldSchemas::new(BTreeMap::from([(relation.name().into(), period_schema)]))?;
                let from = Timestamp(10);
                let to = Timestamp(30);
                let error = tx.write_owned(&overlay, &allowed, None, Some(from), Some(Timestamp::MAX), false).await.unwrap_err();
                assert_eq!(error.code, "rtdb.owned/validity-policy");
                tx.write_owned(&overlay, &allowed, None, Some(from), Some(to), false).await?;
                assert_eq!(tx.read_owned_revision("owner", &overlay, &id, Interval::at(from)?).await?, Some(ordered.system_time));
                assert_eq!(tx.read_owned_revision("owner", &overlay, &id, Interval::at(to)?).await?, None);
                let error = tx.write_owned(&overlay, &withdrawn, None, Some(from), Some(to), false).await.unwrap_err();
                assert_eq!(error.code, "rtdb.owned/repeated-period");
            }
        }
        Ok(())
    }

    #[tokio::test]
    async fn finite_world_unique_claims_check_exact_half_open_shadow_coverage() -> Result<()> {
        use rtdb_types::{EntityId, OwnedRelation, OwnedSchema, OwnedWorld, WorldClockMode, WorldSchemas, WorldShadow};

        struct Parent(Arc<QueryContext>);
        #[async_trait]
        impl WorldConstraintArchive for Parent {
            async fn check(&self, check: ArchivedWorldConstraints<'_>) -> Result<()> {
                check.validate(Arc::clone(&self.0)).await
            }
        }

        let memory = MemoryBudget::new(1 << 24);
        let context = ResolveContext {
            write_event_origins: false,
            placed_receipt_retirement: false,
            owned_witness_scope: Some(rtdb_types::PlacedAttemptScope::new("native-world-unique", 1)?),
            world_constraints: None,
            database: "application".into(),
            storage: Arc::new(BufferPool::new(Arc::new(rtdb_storage::MemoryStore::default()), 1 << 20)),
            memory: Arc::clone(&memory),
            query_options: QueryOptions::default(),
            scan_accelerators: Default::default(),
            cancellation: CancellationToken::new(),
            default_tz: "UTC".into(),
            authorization: Arc::new(NoRoleChecks),
            execution_observer: None,
            tracer: None,
        };
        let state = rtdb_index::PartitionState::empty(Default::default())?;
        let options = SubmitOptions { user: Some("owner".into()), ..Default::default() };
        let relation = OwnedRelation::new("facts")?;
        let schema = OwnedSchema::from_json(
            br#"{"fields":[{"name":"key","type":{"kind":"binary"},"optional":false,"nullable":false}],"validity":"bounded_period"}"#,
        )?;
        let fact = |id: &EntityId| StructFields::from([
            ("_id".into(), id.as_value()), ("key".into(), Value::Binary(b"shared".to_vec())),
        ]);
        let parent_holder = EntityId::String("parent".into());
        let other = EntityId::String("other".into());
        let local = EntityId::String("local".into());
        let cut = TransactionKey { tx_id: TxId(1), system_time: Timestamp(100) };
        let mut root = OpenTransaction::new(state.value().snapshot(), cut, &options, &context)?;
        root.declare_owned_relation(&relation, &schema).await?;
        root.declare_owned_unique_path(&relation, &OwnedRelation::new("by-key")?, "key").await?;
        root.write_owned(&relation, &crate::Documents::new(vec![fact(&parent_holder)], &memory)?,
            None, Some(Timestamp(10)), Some(Timestamp(30)), false).await?;
        root.finish_owned_unique().await?;
        let parent = root.snapshot()?;
        drop(root);
        let archive = parent.clone().with_system_time_cap(Some(cut.system_time))?;
        let archive = context.query_context(archive, cut.system_time, "UTC", None)?;
        let context = ResolveContext { world_constraints: Some(Arc::new(Parent(archive))), ..context };
        let world = Iid([1; 16]);
        let overlay = relation.in_world(world)?;
        let write = |id: &EntityId, from, to, tombstone| -> Result<Operation> {
            let shadow = if tombstone { WorldShadow::Deleted } else { WorldShadow::Present(fact(id)) };
            Ok(Operation::Public(TxOp::OwnedReplaceDocs {
                relation: overlay.clone(), documents: crate::Documents::new(vec![shadow.record(id)?], &memory)?,
                valid_from: Some(Timestamp(from)), valid_to: Some(Timestamp(to)),
            }))
        };
        for mode in [WorldClockMode::Proposal, WorldClockMode::Held { start: cut.system_time }] {
            let record = OwnedWorld {
                ancestor: None, fork_lineage: Vec::new(), id: world, pin: Iid([2; 16]), capture: Iid([3; 16]), suffix: None,
                manifest: "a".repeat(43), basis: cut, parent_publication: None, mode,
                until: Timestamp(1000), dropped: false,
                schemas: WorldSchemas::new(BTreeMap::from([(relation.name().into(), schema.clone())]))?,
            };
            for (start, shadow, another_start, accepted) in [
                (20, None, None, false),
                (20, Some((&other, 20)), None, false),
                (20, Some((&parent_holder, 21)), None, false),
                (20, Some((&parent_holder, 20)), Some(40), true),
                (30, None, Some(39), false),
                (30, None, Some(40), true),
            ] {
                let mut tx = OpenTransaction::new(
                    parent.clone(), TransactionKey { tx_id: TxId(2), system_time: Timestamp(200) },
                    &options, &context,
                )?;
                tx.owned_worlds.insert(world, (record.clone(), memory.reserve(4096)?));
                let mut writes = vec![write(&local, start, 40, false)?];
                if let Some((id, from)) = shadow { writes.push(write(id, from, 30, true)?); }
                if let Some(from) = another_start { writes.push(write(&other, from, 50, false)?); }
                for operation in &writes { tx.apply(operation).await?; }
                let result = tx.check_final_constraints(&writes).await;
                if accepted {
                    result?;
                } else {
                    assert_eq!(result.unwrap_err().code, "rtdb.owned/unique-taken");
                }
            }
        }
        Ok(())
    }

    #[tokio::test]
    async fn native_world_constraints_combine_current_policy_with_inherited_rows_and_local_shadows() -> Result<()> {
        use rtdb_types::{EntityId, OwnedRelation, OwnedSchema, OwnedWorld, WorldClockMode, WorldSchemas, WorldShadow};

        struct Parent(Arc<QueryContext>);
        #[async_trait]
        impl WorldConstraintArchive for Parent {
            async fn check(&self, check: ArchivedWorldConstraints<'_>) -> Result<()> {
                check.validate(Arc::clone(&self.0)).await
            }
        }

        let memory = MemoryBudget::new(1 << 24);
        let context = ResolveContext {
            write_event_origins: false,
            placed_receipt_retirement: false,
            owned_witness_scope: Some(rtdb_types::PlacedAttemptScope::new("native-world-capacity", 1)?),
            world_constraints: None,
            database: "application".into(),
            storage: Arc::new(BufferPool::new(Arc::new(rtdb_storage::MemoryStore::default()), 1 << 20)),
            memory: Arc::clone(&memory),
            query_options: QueryOptions::default(),
            scan_accelerators: Default::default(),
            cancellation: CancellationToken::new(),
            default_tz: "UTC".into(),
            authorization: Arc::new(NoRoleChecks),
            execution_observer: None,
            tracer: None,
        };
        let state = rtdb_index::PartitionState::empty(Default::default())?;
        let options = SubmitOptions { user: Some("owner".into()), ..Default::default() };
        let relation = OwnedRelation::new("facts")?;
        let schema = OwnedSchema::from_json(br#"{"fields":[
            {"name":"partition","type":{"kind":"binary"},"optional":false,"nullable":false},
            {"name":"key","type":{"kind":"binary"},"optional":false,"nullable":false}
        ],"validity":"acceptance"}"#)?;
        let fact = |id: &EntityId, key: &[u8]| StructFields::from([
            ("_id".into(), id.as_value()), ("partition".into(), Value::Binary(b"bucket".to_vec())),
            ("key".into(), Value::Binary(key.to_vec())),
        ]);
        let inherited = EntityId::String("inherited".into());
        let cut = TransactionKey { tx_id: TxId(1), system_time: Timestamp(100) };
        let mut root = OpenTransaction::new(state.value().snapshot(), cut, &options, &context)?;
        root.declare_owned_relation(&relation, &schema).await?;
        root.declare_owned_unique_path(&relation, &OwnedRelation::new("by-key")?, "key").await?;
        root.write_owned(&relation, &crate::Documents::new(vec![fact(&inherited, b"inherited")], &memory)?, None, None, None, false).await?;
        root.finish_owned_unique().await?;
        let parent = root.snapshot()?;
        drop(root);
        let archived = context.query_context(parent.clone(), cut.system_time, "UTC", None)?;
        let context = ResolveContext { world_constraints: Some(Arc::new(Parent(archived))), ..context };
        let mut cap = OpenTransaction::new(
            parent.clone(), TransactionKey { tx_id: TxId(2), system_time: Timestamp(200) }, &options, &context,
        )?;
        cap.declare_owned_capacity(&relation, "partition", 0, 2, &[]).await?;
        cap.finish_owned_capacity().await?;

        let world = Iid([1; 16]);
        let overlay = relation.in_world(world)?;
        let record = OwnedWorld {
            ancestor: None,
            fork_lineage: Vec::new(),
            id: world, pin: Iid([2; 16]), capture: Iid([3; 16]), suffix: None, manifest: "a".repeat(43),
            basis: cut, parent_publication: None, mode: WorldClockMode::Proposal, until: Timestamp(1000), dropped: false,
            schemas: WorldSchemas::new(BTreeMap::from([(relation.name().into(), schema)]))?,
        };
        let write = |shadow: WorldShadow, id: &EntityId| -> Result<Operation> {
            Ok(Operation::Public(TxOp::OwnedReplaceDocs {
                relation: overlay.clone(), documents: crate::Documents::new(vec![shadow.record(id)?], &memory)?,
                valid_from: None, valid_to: None,
            }))
        };
        let one = EntityId::String("one".into());
        let mut duplicate = OpenTransaction::new(
            parent, TransactionKey { tx_id: TxId(2), system_time: Timestamp(150) }, &options, &context,
        )?;
        duplicate.owned_worlds.insert(world, (record.clone(), memory.reserve(4096)?));
        let inherited_key = [write(WorldShadow::Present(fact(&one, b"inherited")), &one)?];
        duplicate.apply(&inherited_key[0]).await?;
        assert_eq!(duplicate.check_final_constraints(&inherited_key).await.unwrap_err().code, "rtdb.owned/unique-taken");

        let held = OwnedWorld {
            mode: WorldClockMode::Held { start: cut.system_time },
            ..record.clone()
        };
        let mut timed = OpenTransaction::new(
            cap.snapshot()?, TransactionKey { tx_id: TxId(3), system_time: Timestamp(300) },
            &options, &context,
        )?;
        timed.owned_worlds.insert(world, (held, memory.reserve(4096)?));
        timed.timed_world_overlays.insert(world, Timestamp(700));
        timed.apply(&inherited_key[0]).await?;
        assert_eq!(
            timed.check_final_constraints(&inherited_key).await.unwrap_err().code,
            "rtdb.owned/unique-taken",
        );

        let first = [write(WorldShadow::Present(fact(&one, b"one")), &one)?];
        let mut accepted = OpenTransaction::new(
            cap.snapshot()?, TransactionKey { tx_id: TxId(3), system_time: Timestamp(300) }, &options, &context,
        )?;
        accepted.owned_worlds.insert(world, (record.clone(), memory.reserve(4096)?));
        accepted.apply(&first[0]).await?;
        accepted.check_final_constraints(&first).await?;

        let two = EntityId::String("two".into());
        let before = accepted.snapshot()?;
        let ordered = TransactionKey { tx_id: TxId(4), system_time: Timestamp(400) };
        let mut duplicate = OpenTransaction::new(before.clone(), ordered, &options, &context)?;
        duplicate.owned_worlds.insert(world, (record.clone(), memory.reserve(4096)?));
        let local_key = [write(WorldShadow::Present(fact(&two, b"one")), &two)?];
        duplicate.apply(&local_key[0]).await?;
        assert_eq!(duplicate.check_final_constraints(&local_key).await.unwrap_err().code, "rtdb.owned/unique-taken");

        let claim = write(WorldShadow::Present(fact(&two, b"two")), &two)?;
        let mut overflow = OpenTransaction::new(before.clone(), ordered, &options, &context)?;
        overflow.owned_worlds.insert(world, (record.clone(), memory.reserve(4096)?));
        overflow.apply(&claim).await?;
        assert_eq!(
            overflow.check_final_constraints(std::slice::from_ref(&claim)).await.unwrap_err().code,
            "rtdb.owned/capacity-exceeded"
        );
        let replacement = [
            write(WorldShadow::Present(fact(&two, b"inherited")), &two)?,
            write(WorldShadow::Deleted, &inherited)?,
        ];
        let mut accepted = OpenTransaction::new(before, ordered, &options, &context)?;
        accepted.owned_worlds.insert(world, (record, memory.reserve(4096)?));
        for write in &replacement { accepted.apply(write).await?; }
        accepted.check_final_constraints(&replacement).await
    }

    #[tokio::test]
    async fn owned_unique_claims_use_the_complete_valid_interval() -> Result<()> {
        use rtdb_types::{EntityId, OwnedRelation, OwnedSchema};

        let memory = MemoryBudget::new(1 << 24);
        let context = ResolveContext {
            write_event_origins: false,
            placed_receipt_retirement: false,
            owned_witness_scope: Some(rtdb_types::PlacedAttemptScope::new("valid-unique", 1)?),
            world_constraints: None,
            database: "application".into(),
            storage: Arc::new(BufferPool::new(Arc::new(rtdb_storage::MemoryStore::default()), 1 << 20)),
            memory: Arc::clone(&memory),
            query_options: QueryOptions::default(),
            scan_accelerators: Default::default(),
            cancellation: CancellationToken::new(),
            default_tz: "UTC".into(),
            authorization: Arc::new(NoRoleChecks),
            execution_observer: None,
            tracer: None,
        };
        let state = rtdb_index::PartitionState::empty(Default::default())?;
        let options = SubmitOptions { user: Some("owner".into()), ..Default::default() };
        let relation = OwnedRelation::new("facts")?;
        let path = OwnedRelation::new("by-key")?;
        let schema = OwnedSchema::from_json(br#"{"fields":[
            {"name":"key","type":{"kind":"binary"},"optional":false,"nullable":false}
        ],"validity":"bounded_period"}"#)?;
        let document = |id: &EntityId| crate::Documents::new(vec![StructFields::from([
            ("_id".into(), id.as_value()),
            ("key".into(), Value::Binary(b"shared".to_vec())),
        ])], &memory);
        let before = EntityId::String("before".into());
        let next = EntityId::String("next".into());
        let correction = EntityId::String("correction".into());
        let first = TransactionKey { tx_id: TxId(1), system_time: Timestamp(100) };
        let mut tx = OpenTransaction::new(state.value().snapshot(), first, &options, &context)?;
        tx.declare_owned_relation(&relation, &schema).await?;
        tx.declare_owned_unique_path(&relation, &path, "key").await?;
        tx.write_owned(&relation, &document(&before)?, None, Some(Timestamp(50)), Some(Timestamp(80)), false).await?;
        tx.write_owned(&relation, &document(&next)?, None, Some(Timestamp(80)), Some(Timestamp(100)), false).await?;
        tx.finish_owned_unique().await?;
        let base = tx.snapshot()?;

        let second = TransactionKey { tx_id: TxId(2), system_time: Timestamp(200) };
        let mut overlapping = OpenTransaction::new(base.clone(), second, &options, &context)?;
        overlapping.write_owned(&relation, &document(&correction)?, None, Some(Timestamp(60)), Some(Timestamp(70)), false).await?;
        assert_eq!(overlapping.finish_owned_unique().await.unwrap_err().code, "rtdb.owned/unique-taken");

        let mut replaced = OpenTransaction::new(base.clone(), second, &options, &context)?;
        replaced.delete_owned(&relation, &before, Some(Timestamp(60)), Some(Timestamp(70))).await?;
        replaced.write_owned(&relation, &document(&correction)?, None, Some(Timestamp(60)), Some(Timestamp(70)), false).await?;
        replaced.finish_owned_unique().await?;
        let corrected = replaced.snapshot()?;
        for (snapshot, at, expected) in [
            (base, 65, Some(&before)),
            (corrected.clone(), 59, Some(&before)),
            (corrected.clone(), 60, Some(&correction)),
            (corrected.clone(), 70, Some(&before)),
            (corrected.clone(), 80, Some(&next)),
            (corrected, 100, None),
        ] {
            let query = context.query_context(snapshot, second.system_time, "UTC", None)?;
            let hit = rtdb_query::lookup_owned_unique(
                query, "owner", &relation, &path, "key", b"shared", Timestamp(at),
            ).await?;
            match expected {
                Some(id) => {
                    let hit = hit.expect("expected a unique holder");
                    assert_eq!(rtdb_query::owned_row_revision(hit.row.batch(), 0, id, &memory)?, hit.revision);
                },
                None => assert!(hit.is_none()),
            }
        }
        Ok(())
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
                events.push(StoredEvent { origin: None, commit_position: 0, entity_key: None, iid: Iid::from_slice(&iid).unwrap(), system_from: system_time, valid_time, operation, recency: None });
            }
        }
        events
    }

    #[test]
    fn scalar_mutation_put_stages_the_same_events_without_retaining_the_query_batch() {
        use arrow_array::{ArrayRef, FixedSizeBinaryArray, Int64Array, StructArray, TimestampMicrosecondArray, types::Int64Type};
        use arrow_schema::{Field, TimeUnit as ArrowTimeUnit};

        let memory = MemoryBudget::new(1 << 24);
        let ids = Arc::new(Int64Array::from(vec![1, 2]));
        let make_documents = |cells| StructArray::try_new(vec![
            Arc::new(Field::new("_id", DataType::Int64, false)),
            Arc::new(Field::new("n", DataType::Int64, true)),
            Arc::new(Field::new("nulls", DataType::Int64, true)),
        ].into(), vec![ids.clone(), Arc::new(Int64Array::from(cells)), Arc::new(Int64Array::from(vec![None::<i64>; 2]))], None).unwrap();
        let iids = FixedSizeBinaryArray::try_from_iter([[1_u8; 16], [2; 16]].into_iter()).unwrap();
        let times = DataType::Timestamp(ArrowTimeUnit::Microsecond, Some("UTC".into()));
        let make_columns = |cells| -> Vec<ArrayRef> { vec![Arc::new(iids.clone()),
            Arc::new(TimestampMicrosecondArray::from(vec![None, Some(1)]).with_timezone("UTC")),
            Arc::new(TimestampMicrosecondArray::from(vec![Some(12), None]).with_timezone("UTC")),
            Arc::new(make_documents(cells)),
        ] };
        let columns = make_columns(vec![Some(7), None]);
        let schema = Arc::new(Schema::new(vec![Field::new("_iid", DataType::FixedSizeBinary(16), false),
            Field::new("_valid_from", times.clone(), true), Field::new("_valid_to", times, true),
            Field::new("doc", columns[3].data_type().clone(), false),
        ]));
        let output = RecordBatch::try_new(Arc::clone(&schema), columns).unwrap();
        let system_time = Timestamp(10);
        let mut events = Vec::new();
        mutation_events(&output, MutationKind::Put, system_time, |event| { events.push(event); Ok(()) }).unwrap();
        let expected = rtdb_index::IndexBatch::copy_events(&events, &StructFields::new(), &memory).unwrap();

        let put = mutation_put_batch(&output, system_time, &memory).unwrap().unwrap();

        assert_eq!(put.batch(), expected.arrow());
        let content = put.batch().column_by_name("op").unwrap().as_union().child(2).as_struct();
        assert_ne!(content.column(0).as_primitive::<Int64Type>().values().as_ptr(), ids.values().as_ptr());

        let null_first = RecordBatch::try_new(Arc::clone(&schema), make_columns(vec![None, Some(7)])).unwrap();
        assert!(mutation_put_batch(&null_first, system_time, &memory).unwrap().is_none());

        let mut invalid_times = make_columns(vec![Some(7), None]);
        invalid_times[2] = Arc::new(TimestampMicrosecondArray::from(vec![Some(12), Some(0)]).with_timezone("UTC"));
        let invalid_times = RecordBatch::try_new(schema, invalid_times).unwrap();
        assert!(mutation_put_batch(&invalid_times, system_time, &memory).unwrap().is_none());
        let error = mutation_events(&invalid_times, MutationKind::Put, system_time, |_| {
            memory.reserve(memory.limit_bytes())?;
            Ok(())
        }).unwrap_err();
        assert_eq!(error.code, "rtdb/memory-budget-exceeded");
    }

    #[test]
    fn nested_dml_payloads_use_arrow_with_distinct_per_row_valid_times() {
        use arrow_array::{ArrayRef, FixedSizeBinaryArray, TimestampMicrosecondArray};
        use arrow_schema::Field;

        let memory = MemoryBudget::new(1 << 24);
        let docs = (1i64..=2).map(|id| Value::Struct(StructFields::from([
            ("_id".into(), Value::I64(id)),
            ("value".into(), Value::Struct(StructFields::from([
                ("dates".into(), Value::list(vec![Value::I64(id * 10), Value::I64(id * 11)])),
                ("flags".into(), Value::FixedSizeList(vec![Value::Bool(id == 1), Value::Bool(true)].into())),
            ]))),
        ]))).collect::<Vec<_>>();
        let doc_type = docs[0].logical_type();
        let doc_field = rtdb_columnar::to_field("doc", &doc_type).unwrap();
        let iids = (1i64..=2).map(|id| rtdb_types::EntityId::Integer(id).iid().0).collect::<Vec<_>>();
        let columns: Vec<ArrayRef> = vec![
            Arc::new(FixedSizeBinaryArray::try_from_iter(iids.iter()).unwrap()),
            Arc::new(TimestampMicrosecondArray::from(vec![12, 50]).with_timezone("UTC")),
            Arc::new(TimestampMicrosecondArray::from(vec![20, 75]).with_timezone("UTC")),
            rtdb_columnar::copy_array(&doc_type, &docs).unwrap(),
        ];
        let fields = vec![
            Field::new("_iid", columns[0].data_type().clone(), false),
            Field::new("_valid_from", columns[1].data_type().clone(), false),
            Field::new("_valid_to", columns[2].data_type().clone(), false),
            doc_field,
        ];
        let query_memory = MemoryBudget::new(1 << 24);
        let batch = RecordBatch::try_new(Arc::new(Schema::new(fields)), columns).unwrap();
        let batch = OwnedBatch::new(batch, &query_memory).unwrap();
        let system_time = Timestamp(10);
        let mut events = Vec::new();
        mutation_events(batch.batch(), MutationKind::Put, system_time, |event| { events.push(event); Ok(()) }).unwrap();
        let expected = rtdb_index::IndexBatch::copy_events(&events, &StructFields::new(), &memory).unwrap();
        let output = mutation_put_batch(batch.batch(), system_time, &memory).unwrap().expect("Expected Arrow mutation payload");
        drop(batch);
        assert_eq!(query_memory.used_bytes(), 0);
        assert_eq!(output.batch(), expected.arrow());
        assert_eq!(events[0].valid_time, Interval::from_micros(12, 20).unwrap());
        assert_eq!(events[1].valid_time, Interval::from_micros(50, 75).unwrap());
    }

    #[tokio::test]
    async fn a_mutation_read_a_column_at_a_time_writes_the_events_a_row_by_row_read_does() {
        let memory = MemoryBudget::new(1 << 26);
        let resolve = ResolveContext {
            write_event_origins: false,
            placed_receipt_retirement: false,
            owned_witness_scope: None,
            world_constraints: None,
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
