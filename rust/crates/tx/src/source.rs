use rtdb_types::StructFields;
use std::{borrow::Cow, collections::BTreeMap};
use std::sync::Arc;

use arrow_array::{Array, ArrayRef, FixedSizeBinaryArray, ListArray, RecordBatch, StructArray, UnionArray, cast::AsArray};
use arrow_buffer::{Buffer, OffsetBuffer, ScalarBuffer};
use arrow_schema::{DataType, Field, Schema, UnionFields, UnionMode};
use rtdb_columnar::{BatchBuilder, MemoryBudget, MemoryReservation, OwnedBatch, build_array, copy_array, estimate_builder_bytes, estimate_builder_bytes_refs, estimate_read_value_bytes, from_field, read_value, struct_field_cell, to_field};
use rtdb_format::envelope::{SourceBody, SourceMessage};
use prost::Message;
use rtdb_types::{Anomaly, EntityId, Iid, LogicalType, OrderedSet, PlacedAttempt, PlacedAttemptSeed, Result, ScalarType, SourcePosition, TableRef, TimeUnit, Timestamp, Value};

use crate::{ArrowDocuments, Documents, EntityIds, ReplaySubmission, RetainedBytes, SqlArguments, SubmitOptions, TxOp, UserMetadata};
use crate::bounded_retry::{BOUNDED_RETRY_METADATA_KEY, BoundedRetry};
use crate::values::{DecodedDocumentLayout, contains_fixed_size_list};

const LEGACY_REPLAY_ABORT: &str = "ASSERT FALSE";

#[derive(Clone, Debug)]
pub(crate) enum Operation {
    Public(TxOp),
    OwnedWriteIids { create_only: bool, relation: rtdb_types::OwnedRelation, documents: Documents, iids: Vec<Iid>, valid_from: Option<Timestamp>, valid_to: Option<Timestamp> },
    InsertIids { table: TableRef, documents: Documents, iids: Vec<Iid>, valid_from: Option<Timestamp>, valid_to: Option<Timestamp> },
    InsertArrowIids { table: TableRef, documents: ArrowDocuments, iids: Vec<Iid>, valid_from: Option<Timestamp>, valid_to: Option<Timestamp> },
    PutIids { table: TableRef, documents: Documents, iids: Vec<Iid>, valid_from: Option<Timestamp>, valid_to: Option<Timestamp> },
    PutReplayArrow { table: TableRef, documents: ArrowDocuments, iids: Option<Vec<Iid>>, valid_from: Option<Timestamp>, valid_to: Option<Timestamp> },
    PatchIids { table: TableRef, documents: Documents, iids: Vec<Iid>, valid_from: Option<Timestamp>, valid_to: Option<Timestamp> },
    PatchArrowIids { table: TableRef, documents: ArrowDocuments, iids: Vec<Iid>, valid_from: Option<Timestamp>, valid_to: Option<Timestamp> },
    Removed(String),
}

impl Operation {
    pub(crate) fn owned_relation(&self) -> Option<&rtdb_types::OwnedRelation> {
        match self {
            Self::Public(TxOp::OwnedDeclareRelation { relation, .. } | TxOp::OwnedDeclarePath { relation, .. } | TxOp::OwnedDeclareSpatialPath { relation, .. } | TxOp::OwnedDropSpatialPath { relation, .. }
                | TxOp::OwnedDeclareSearchPath { relation, .. } | TxOp::OwnedDeclareUniquePath { relation, .. }
                | TxOp::OwnedDeclareFixedRange { relation, .. } | TxOp::OwnedDeclareCapacity { relation, .. } | TxOp::OwnedCheckCapacity { relation, .. } | TxOp::OwnedCheckRange { relation, .. } | TxOp::OwnedCheck { relation, .. } | TxOp::OwnedCheckWorldPurge { relation, .. }
                | TxOp::OwnedErase { relation, .. } | TxOp::OwnedSweepExpired { relation, .. } | TxOp::OwnedDelete { relation, .. } | TxOp::OwnedInsertDocs { relation, .. }
                | TxOp::OwnedReplaceDocs { relation, .. }) | Self::OwnedWriteIids { relation, .. } => Some(relation),
            _ => None,
        }
    }

    pub(crate) fn is_owned(&self) -> bool {
        match self { Self::Public(operation) => operation.is_owned(), Self::OwnedWriteIids { .. } => true, _ => false }
    }
}

pub struct SourceTransaction {
    pub(crate) operations: Vec<Operation>,
    pub options: SubmitOptions,
    pub(crate) guard: Option<SourceGuard>,
    placed_seed: Option<PlacedAttemptSeed>,
    pub(crate) memory: Arc<MemoryBudget>,
    _reservation: Arc<MemoryReservation>,
}

pub(crate) enum SourceGuard {
    Application(IdempotencyGuard),
    Replay(ReplaySubmission),
    Bounded(BoundedRetry),
}

pub(crate) struct IdempotencyGuard {
    pub(crate) submitted_at: Timestamp,
    pub(crate) after_bootstrap: bool,
}

impl SourceTransaction {
    pub fn uses_staging_deadline(&self) -> bool {
        self.operations.iter().any(|operation| matches!(operation,
            Operation::Public(TxOp::OwnedDeadline { recheck_at_staging: true, .. })))
    }

    pub fn new(ops: Vec<TxOp>, options: SubmitOptions, memory: &Arc<MemoryBudget>) -> Result<Self> {
        let operations = ops.into_iter().map(Operation::Public).collect();
        Self::from_operations(operations, options, memory)?.with_bounded_retry()
    }
    pub(crate) fn from_operations(operations: Vec<Operation>, options: SubmitOptions, memory: &Arc<MemoryBudget>) -> Result<Self> {
        let mut bytes = operations.capacity().checked_mul(std::mem::size_of::<Operation>()).and_then(|size| size.checked_add(options.default_tz.capacity())).and_then(|size| size.checked_add(options.user.as_ref().map_or(0, String::capacity))).ok_or_else(overflow)?;
        for operation in &operations {
            let (text, table, ids) = match operation {
                Operation::Public(TxOp::OwnedDeclareSearchPath { relation, name, column } | TxOp::OwnedDeclareUniquePath { relation, name, column }) =>
                    (owned_path_bytes(relation, name, std::slice::from_ref(column))?, None, 0),
                Operation::Public(TxOp::OwnedCheckCapacity { relation, column, prefix, .. }) =>
                    (capacity_condition_bytes(relation, column.capacity(), prefix.capacity())?, None, 0),
                Operation::Public(TxOp::OwnedDeclareCapacity { relation, column, when, .. }) =>
                    (capacity_condition_bytes(relation, column.capacity(), when.capacity())?, None, 0),
                Operation::Public(TxOp::OwnedDropSpatialPath { relation, name }) =>
                    (relation.name().len() + name.name().len(), None, 0),
                Operation::Public(TxOp::OwnedDeclareRelation { relation, schema }) =>
                    (relation.name().len().checked_add(schema.retained_bytes()).ok_or_else(overflow)?, None, 0),
                Operation::Public(TxOp::OwnedDeclarePath { relation, name, columns } | TxOp::OwnedDeclareSpatialPath { relation, name, columns }) =>
                    (owned_path_bytes(relation, name, columns)?, None, 0),
                Operation::Public(TxOp::HostedControl(control)) => (control.retained_bytes()?, None, 0),
                Operation::Public(TxOp::OwnedForkWorld { record, attempt, .. }) => (Value::document_retained_bytes(record)?.saturating_add(attempt.len()), None, 128),
                Operation::Public(TxOp::OwnedPublishRoot { attempt, .. } | TxOp::OwnedPublishWorld { attempt, .. }) => (attempt.len(), None, 128),
                Operation::Public(TxOp::OwnedCheckWorldPurge { relation, .. }) => (relation.name().len(), None, 0),
                Operation::Public(TxOp::OwnedDeadline { .. } | TxOp::OwnedCheckWorldScope { .. } | TxOp::HostedConstraintRefusal) => (0, None, 0),
                Operation::Public(TxOp::OwnedDeclareFixedRange { relation, rule }) =>
                    (relation.name().len() + rule.path.capacity() + rule.column.capacity(), None, 0),
                Operation::Public(TxOp::OwnedCheckRange { relation, fixed, .. }) =>
                    (range_evidence_bytes(relation, fixed), None, 0),
                Operation::Public(TxOp::OwnedCheck { relation, id, .. } | TxOp::OwnedDelete { relation, id, .. } | TxOp::OwnedErase { relation, id } | TxOp::OwnedSweepExpired { relation, id, .. }) =>
                    (relation.name().len().checked_add(id.as_value().retained_bytes()?).ok_or_else(overflow)?, None, 0),
                Operation::Public(TxOp::OwnedInsertDocs { relation, .. } | TxOp::OwnedReplaceDocs { relation, .. }) => (relation.name().len(), None, 0),
                Operation::OwnedWriteIids { relation, iids, .. } => (relation.name().len(), None, iids.capacity().checked_mul(16).ok_or_else(overflow)?),
                Operation::Public(TxOp::Sql { sql, .. } | TxOp::SqlBytes { sql, .. }) => (sql.capacity(), None, 0),
                Operation::Public(TxOp::InsertDocs { table, .. } | TxOp::PutDocs { table, .. } | TxOp::PutArrowDocs { table, .. } | TxOp::PatchDocs { table, .. } | TxOp::DeleteDocs { table, .. } | TxOp::EraseDocs { table, .. }) => (0, Some(table), 0),
                Operation::InsertIids { table, iids, .. } | Operation::InsertArrowIids { table, iids, .. } | Operation::PutIids { table, iids, .. } | Operation::PatchIids { table, iids, .. } | Operation::PatchArrowIids { table, iids, .. } => (0, Some(table), iids.capacity().checked_mul(16).ok_or_else(overflow)?),
                Operation::PutReplayArrow { table, iids, .. } => {
                    (0, Some(table), iids.as_ref().map_or(0, Vec::capacity).checked_mul(16).ok_or_else(overflow)?)
                }
                Operation::Removed(name) => (name.capacity(), None, 0),
            };
            bytes = bytes.checked_add(text).and_then(|size| size.checked_add(ids)).and_then(|size| size.checked_add(table.map_or(0, |table| table.schema.capacity() + table.table.capacity()))).and_then(|size| size.checked_add(256)).ok_or_else(overflow)?;
        }
        let reservation = memory.reserve(bytes)?;
        Ok(Self { operations, options, guard: None, placed_seed: None, memory: Arc::clone(memory), _reservation: reservation })
    }

    pub fn with_idempotency_guard(mut self, submitted_at: Timestamp, after_bootstrap: bool) -> Self {
        self.guard = Some(SourceGuard::Application(IdempotencyGuard { submitted_at, after_bootstrap }));
        self
    }

    pub fn with_replay_submission(mut self, replay: ReplaySubmission) -> Self {
        self.guard = Some(SourceGuard::Replay(replay));
        self
    }

    pub(crate) fn with_bounded_retry(mut self) -> Result<Self> {
        self.guard = BoundedRetry::from_metadata(self.options.user_metadata.as_ref())?.map(SourceGuard::Bounded);
        Ok(self)
    }

    pub fn with_placed_seed(mut self, seed: Option<PlacedAttemptSeed>) -> Self {
        self.placed_seed = seed;
        self
    }

    pub(crate) fn placed_attempt(&self, position: SourcePosition) -> Result<Option<PlacedAttempt>> {
        self.placed_seed.map(|seed| seed.identify(position, self.options.user.as_deref())).transpose()
    }

    pub fn skipped_attempt(message: &SourceMessage, position: SourcePosition) -> Result<Option<PlacedAttempt>> {
        let SourceMessage::Protobuf(SourceBody::Tx(tx)) = message else { return Ok(None); };
        decode_placed_seed(tx)?.map(|seed| seed.identify(position, tx.user.as_deref())).transpose()
    }

    /// Decodes current flat tx operations and the legacy one-row envelope without expanding SQL.
    pub fn decode(message: &SourceMessage, memory: &Arc<MemoryBudget>) -> Result<Self> {
        let option_bytes = match message {
            SourceMessage::Protobuf(SourceBody::Tx(tx)) => tx.default_tz.as_ref().map_or(0, String::len)
                .checked_add(tx.user.as_ref().map_or(0, String::len)).ok_or_else(overflow)?,
            _ => 0,
        };
        let _option_scratch = memory.reserve(option_bytes)?;
        let placed_seed = match message {
            SourceMessage::Protobuf(SourceBody::Tx(tx)) => decode_placed_seed(tx)?,
            _ => None,
        };
        let (pages, mut options, guard, legacy, abort_on_legacy_resolver) = match message {
            SourceMessage::Protobuf(SourceBody::Tx(tx)) => {
                let bytes = tx.tx_ops.as_deref().ok_or_else(|| invalid("Transaction has no operations stream"))?;
                let options = SubmitOptions { default_tz: tx.default_tz.clone().ok_or_else(|| invalid("Transaction has no default timezone"))?, system_time: tx.system_time_micros.map(Timestamp), user: tx.user.clone(), user_metadata: tx.user_metadata.as_deref().filter(|bytes| !bytes.is_empty()).map(|bytes| decode_metadata(bytes, memory)).transpose()? };
                let guard = match (&tx.idempotency_guard, &tx.replay_submission, &tx.bounded_retry) {
                    (Some(guard), None, None) => {
                        let submitted_at = Timestamp(guard.submitted_at_micros.ok_or_else(|| invalid("Idempotency guard has no submission timestamp"))?);
                        let after_bootstrap = guard.after_bootstrap.ok_or_else(|| invalid("Idempotency guard has no operation position"))?;
                        Some(SourceGuard::Application(IdempotencyGuard { submitted_at, after_bootstrap }))
                    }
                    (None, Some(replay), None) => {
                        let key = replay.key.as_deref().ok_or_else(|| invalid("Replay submission has no key"))?;
                        let submitted_at = Timestamp(replay.submitted_at_micros.ok_or_else(|| invalid("Replay submission has no submission timestamp"))?);
                        let not_after = Timestamp(replay.not_after_micros.ok_or_else(|| invalid("Replay submission has no cutoff"))?);
                        let replay = ReplaySubmission::admit(key, submitted_at, not_after)
                            .map_err(|error| invalid(format!("Invalid replay submission: {error}")))?;
                        Some(SourceGuard::Replay(replay))
                    }
                    (None, None, Some(retry)) => {
                        let nonce = retry.nonce.as_deref().ok_or_else(|| invalid("Bounded retry has no nonce"))?;
                        let cutoff = retry.not_after_micros.ok_or_else(|| invalid("Bounded retry has no cutoff"))?;
                        let identity = BoundedRetry::new(nonce, cutoff)?;
                        if BoundedRetry::from_metadata(options.user_metadata.as_ref())? != Some(identity) {
                            return Err(invalid("Bounded retry disagrees with transaction metadata"));
                        }
                        Some(SourceGuard::Bounded(identity))
                    }
                    (None, None, None) => None,
                    _ => return Err(invalid("Source transaction contains multiple retry guards")),
                };
                let abort_on_legacy_resolver = match tx.replay_submission.as_ref().and_then(|replay| replay.abort_on_legacy_resolver) {
                    Some(true) => true,
                    Some(false) => return Err(invalid("Replay submission explicitly disables the legacy resolver assertion")),
                    None => false,
                };
                (crate::ipc::decode(bytes, memory)?, options, guard, false, abort_on_legacy_resolver)
            }
            SourceMessage::LegacyTransaction(bytes) => (crate::ipc::decode(bytes, memory)?, SubmitOptions::default(), None, true, false),
            _ => return Err(invalid("Source message is not a transaction")),
        };
        if pages.is_empty() && !legacy {
            if guard.is_some() { return Err(invalid("A guarded transaction requires an operations batch")); }
            let mut transaction = Self::from_operations(Vec::new(), options, memory)?;
            transaction.placed_seed = placed_seed;
            return Ok(transaction);
        }
        let first = pages.first().ok_or_else(|| invalid("Transaction IPC stream has no record batch"))?;
        let mut parsed_fields = memory.reserve(4096)?;
        let parsed_fields = Arc::get_mut(&mut parsed_fields).expect("source decoding owns its reservation");
        if legacy {
            if first.batch().num_rows() != 1 || pages.len() != 1 { return Err(invalid("Legacy transaction must have one envelope row")); }
            let batch = first.batch();
            let option_fields = ["default-tz", "system-time", "user", "user-metadata"];
            let bound = batch.schema().fields().iter().zip(batch.columns()).filter(|(field, _)| option_fields.contains(&field.name().as_str()))
                .try_fold(parsed_fields.bytes(), |bytes, (field, column)| {
                    bytes.checked_add(estimate_read_value_bytes(field, column.as_ref(), 0)?).ok_or_else(overflow)
                })?;
            parsed_fields.grow_to(bound)?;
            options.default_tz = match cell(first.batch(), "default-tz", 0)? { Value::Utf8(value) => value, _ => return Err(invalid("Legacy transaction has no timezone")) };
            options.system_time = optional_timestamp(cell(first.batch(), "system-time", 0)?)?;
            options.user = optional_string(optional_cell(first.batch(), "user", 0)?)?;
            let metadata = optional_cell(first.batch(), "user-metadata", 0)?;
            if !matches!(metadata, Value::Null | Value::Undefined) { options.user_metadata = Some(UserMetadata::new(metadata, memory)?); }
        }
        let mut reader = SourceOperationReader { memory, parsed_fields, layouts: BTreeMap::new() };
        let mut operations = Vec::new();
        for page in &pages {
            let batch = page.batch();
            let column = batch.column(batch.schema().index_of("tx-ops").map_err(|error| invalid(error.to_string()))?);
            let array: ArrayRef = if legacy {
                let list = column.as_any().downcast_ref::<ListArray>().ok_or_else(|| invalid("Legacy tx-ops is not a list"))?;
                list.value(0)
            } else { Arc::clone(column) };
            let union = array.as_any().downcast_ref::<UnionArray>().ok_or_else(|| invalid("Transaction operations are not a union"))?;
            let source_bytes = page.reservation().bytes();
            for row in 0..union.len() { operations.push(reader.operation(union, row, source_bytes)?); }
        }
        if abort_on_legacy_resolver || matches!(guard, Some(SourceGuard::Bounded(_))) {
            if !matches!(operations.first(), Some(Operation::Public(TxOp::Sql { sql, args: None })) if sql == LEGACY_REPLAY_ABORT) {
                return Err(invalid("Replay submission has no legacy resolver assertion"));
            }
            operations.remove(0);
        }
        if let Some(SourceGuard::Application(guard)) = &guard {
            let operation = operations.get(usize::from(guard.after_bootstrap));
            if !matches!(operation, Some(Operation::Public(TxOp::Sql { args: None, .. }))) {
                return Err(invalid("Idempotency guard does not name a SQL operation without arguments"));
            }
        }
        let mut transaction = Self::from_operations(operations, options, memory)?;
        transaction.guard = guard;
        transaction.placed_seed = placed_seed;
        Ok(transaction)
    }

    /// Encodes the operations as supplied; submit-time SQL expansion is a separate caller operation.
    /// The retry identity's key, when the transaction's metadata names one.
    pub(crate) fn idempotency_key(&self) -> Option<&str> {
        let Value::Struct(fields) = self.options.user_metadata.as_ref()?.value() else { return None };
        match fields.get("idempotency_key")? { Value::Utf8(key) => Some(key), _ => None }
    }

    /// A digest of everything that determines this transaction's effects, apart from its idempotency key.
    /// Every replica computes it from the same logged record, so the retry decision is deterministic.
    pub(crate) fn body_digest(&self) -> Result<String> {
        use sha2::{Digest, Sha256};
        let mut hash = Sha256::new();
        let mut field = |bytes: &[u8]| { hash.update((bytes.len() as u64).to_be_bytes()); hash.update(bytes); };
        let batch = self.operation_batch(None, SourcePurpose::Receipt)?;
        field(crate::ipc::encode(batch.batch().schema().as_ref(), [batch.batch()], &self.memory)?.as_slice());
        field(self.options.default_tz.as_bytes());
        field(&self.options.system_time.map_or([0; 9], |time| { let mut bytes = [1; 9]; bytes[1..].copy_from_slice(&time.0.to_be_bytes()); bytes }));
        if let Some(Value::Struct(fields)) = self.options.user_metadata.as_ref().map(UserMetadata::value) {
            let mut rest = fields.clone();
            rest.remove("idempotency_key");
            if matches!(self.guard, Some(SourceGuard::Bounded(_))) { rest.remove(BOUNDED_RETRY_METADATA_KEY); }
            field(encode_metadata(&UserMetadata::new(Value::Struct(rest), &self.memory)?, &self.memory)?.as_slice());
        }
        Ok(hash.finalize().iter().map(|byte| format!("{byte:02x}")).collect())
    }

    pub fn encode(&self) -> Result<RetainedBytes> {
        let bounded = BoundedRetry::from_metadata(self.options.user_metadata.as_ref())?;
        let attached = match &self.guard { Some(SourceGuard::Bounded(retry)) => Some(*retry), _ => None };
        if bounded != attached {
            return Err(invalid("Bounded retry cannot share another retry guard"));
        }
        let prefix = matches!(self.guard, Some(SourceGuard::Replay(_) | SourceGuard::Bounded(_))).then(|| {
            Operation::Public(TxOp::Sql { sql: LEGACY_REPLAY_ABORT.into(), args: None })
        });
        let batch = self.operation_batch(prefix.as_ref(), SourcePurpose::Log)?;
        let bytes = crate::ipc::encode(batch.batch().schema().as_ref(), [batch.batch()], &self.memory)?;
        let metadata = self.options.user_metadata.as_ref().map(|metadata| encode_metadata(metadata, &self.memory)).transpose()?;
        let body_size = bytes.as_slice().len().checked_add(metadata.as_ref().map_or(0, |bytes| bytes.as_slice().len())).and_then(|bytes| bytes.checked_add(self.options.default_tz.capacity())).and_then(|bytes| bytes.checked_add(self.options.user.as_ref().map_or(0, String::capacity))).and_then(|bytes| bytes.checked_add(4096)).ok_or_else(overflow)?;
        let _body_reservation = self.memory.reserve(body_size)?;
        use rtdb_format::generated::xtdb::log::proto;
        let (idempotency_guard, replay_submission, bounded_retry) = match &self.guard {
            Some(SourceGuard::Application(guard)) => (Some(proto::IdempotencyGuard {
                submitted_at_micros: Some(guard.submitted_at.0),
                after_bootstrap: Some(guard.after_bootstrap),
            }), None, None),
            Some(SourceGuard::Replay(replay)) => (None, Some(proto::ReplaySubmission {
                key: Some(replay.key().to_owned()),
                submitted_at_micros: Some(replay.submitted_at().0),
                not_after_micros: Some(replay.not_after().0),
                abort_on_legacy_resolver: Some(true),
            }), None),
            Some(SourceGuard::Bounded(retry)) => (None, None, Some(proto::BoundedRetry {
                nonce: Some(retry.nonce().as_bytes().to_vec()),
                not_after_micros: Some(retry.cutoff().0),
            })),
            None => (None, None, None),
        };
        let tx = rtdb_format::generated::xtdb::log::proto::Tx {
            tx_ops: Some(bytes.as_slice().to_vec()), system_time_micros: self.options.system_time.map(|time| time.0),
            default_tz: Some(self.options.default_tz.clone()), user: self.options.user.clone(),
            user_metadata: metadata.as_ref().map(|bytes| bytes.as_slice().to_vec()), external_source_token: None,
            idempotency_guard,
            replay_submission,
            bounded_retry,
            placed_attempt_seed: self.placed_seed.map(|seed| seed.encode().to_vec()),
        };
        let wire = rtdb_format::generated::xtdb::log::proto::SourceLogMessage { message: Some(SourceBody::Tx(tx)) };
        let reservation = self.memory.reserve(wire.encoded_len().checked_add(128).ok_or_else(overflow)?)?;
        let encoded = SourceMessage::Protobuf(wire.message.ok_or_else(|| invalid("Source envelope has no transaction"))?).encode()?;
        RetainedBytes::with_reservation(encoded, reservation)
    }

    fn operation_batch(&self, prefix: Option<&Operation>, purpose: SourcePurpose) -> Result<OwnedBatch> {
        let operations = || prefix.into_iter().chain(&self.operations);
        let scratch = operations().try_fold(65_536usize, |bytes, operation| {
            let extra = match operation {
                Operation::Public(TxOp::OwnedInsertDocs { documents, .. } | TxOp::OwnedReplaceDocs { documents, .. } | TxOp::InsertDocs { documents, .. } | TxOp::PutDocs { documents, .. } | TxOp::PatchDocs { documents, .. }) | Operation::OwnedWriteIids { documents, .. } | Operation::InsertIids { documents, .. } | Operation::PutIids { documents, .. } | Operation::PatchIids { documents, .. } => documents.rows().iter().try_fold(0usize, |bytes, document| bytes.checked_add(Value::document_retained_bytes(document)?).ok_or_else(overflow))?,
                Operation::Public(TxOp::PutArrowDocs { documents, .. }) | Operation::InsertArrowIids { documents, .. } | Operation::PutReplayArrow { documents, .. } | Operation::PatchArrowIids { documents, .. } => {
                    documents.array().len().checked_mul(64).ok_or_else(overflow)?
                }
                Operation::Public(TxOp::Sql { sql, args }) => {
                    let rows = args.as_ref().map(SqlArguments::value_bytes).transpose()?.unwrap_or(0);
                    rows.checked_add(sql.capacity()).ok_or_else(overflow)?
                }
                Operation::Public(TxOp::SqlBytes { sql, arg_bytes }) => sql.capacity().checked_add(arg_bytes.as_slice().len()).ok_or_else(overflow)?,
                Operation::Public(TxOp::DeleteDocs { ids, .. } | TxOp::EraseDocs { ids, .. }) => ids.ids().len().checked_mul(128).ok_or_else(overflow)?,
                Operation::Public(TxOp::OwnedDeclareSearchPath { relation, name, column } | TxOp::OwnedDeclareUniquePath { relation, name, column }) =>
                    owned_path_bytes(relation, name, std::slice::from_ref(column))?,
                Operation::Public(TxOp::OwnedCheckCapacity { relation, column, prefix, .. }) =>
                    capacity_condition_bytes(relation, column.capacity(), prefix.capacity())?,
                Operation::Public(TxOp::OwnedDeclareCapacity { relation, column, when, .. }) =>
                    capacity_condition_bytes(relation, column.capacity(), when.capacity())?,
                Operation::Public(TxOp::OwnedDropSpatialPath { relation, name }) =>
                    relation.name().len() + name.name().len(),
                Operation::Public(TxOp::OwnedDeclareRelation { relation, schema }) =>
                    relation.name().len().checked_add(schema.retained_bytes()).ok_or_else(overflow)?,
                Operation::Public(TxOp::OwnedDeclarePath { relation, name, columns } | TxOp::OwnedDeclareSpatialPath { relation, name, columns }) =>
                    owned_path_bytes(relation, name, columns)?,
                Operation::Public(TxOp::HostedControl(control)) => control.retained_bytes()?,
                Operation::Public(TxOp::OwnedForkWorld { record, attempt, .. }) => Value::document_retained_bytes(record)?.saturating_add(attempt.len() + 128),
                Operation::Public(TxOp::OwnedPublishRoot { attempt, .. } | TxOp::OwnedPublishWorld { attempt, .. }) => attempt.len() + 128,
                Operation::Public(TxOp::OwnedCheckWorldPurge { relation, .. }) => relation.name().len(),
                Operation::Public(TxOp::OwnedDeadline { .. } | TxOp::OwnedCheckWorldScope { .. } | TxOp::HostedConstraintRefusal) => 0,
                Operation::Public(TxOp::OwnedDeclareFixedRange { relation, rule }) =>
                    relation.name().len() + rule.path.capacity() + rule.column.capacity(),
                Operation::Public(TxOp::OwnedCheckRange { relation, fixed, .. }) => range_evidence_bytes(relation, fixed),
                Operation::Public(TxOp::OwnedCheck { relation, id, .. } | TxOp::OwnedDelete { relation, id, .. } | TxOp::OwnedErase { relation, id } | TxOp::OwnedSweepExpired { relation, id, .. }) =>
                    relation.name().len().checked_add(id.as_value().retained_bytes()?).ok_or_else(overflow)?,
                Operation::Removed(name) => name.capacity(),
            };
            // An operation's own body is a few small fields beside what `extra` counts; a transaction of many small operations is charged for those, not for a fixed allowance each.
            bytes.checked_add(extra.checked_mul(8).and_then(|bytes| bytes.checked_add(OPERATION_BODY_BYTES)).ok_or_else(overflow)?).ok_or_else(overflow)
        })?;
        let _scratch_reservation = self.memory.reserve(scratch)?;
        let mut entries = Vec::new();
        for operation in operations() {
            check_writable_table(operation)?;
            if matches!(operation, Operation::Public(TxOp::DeleteDocs { ids, .. } | TxOp::EraseDocs { ids, .. }) if ids.ids().is_empty()) { continue; }
            entries.push(operation_value(operation, &self.memory, purpose)?);
        }
        // Operations on one table with the same columns share one document schema, so its fields are charged once.
        // Each operation still adds its cells to the type, offset and list buffers that hold the operations together.
        let mut charged: std::collections::BTreeSet<(&str, &[String])> = Default::default();
        let metadata_bound = entries.iter().try_fold(65_536usize, |bytes, (_, entry)| {
            let bytes = bytes.checked_add(OPERATION_CELL_BYTES).ok_or_else(overflow)?;
            let Some((table, doc)) = &entry.documents else { return Ok(bytes); };
            if !charged.insert((table.as_str(), doc.columns())) { return Ok(bytes); }
            doc.columns().iter().try_fold(bytes, |bytes, name| {
                let size = name.len().checked_mul(8).and_then(|size| size.checked_add(1024)).ok_or_else(overflow)?;
                bytes.checked_add(size).ok_or_else(overflow)
            })
        })?;
        let mut arrays = BatchBuilder::new(metadata_bound, &self.memory)?;
        let union = named_union(entries, |leg| operation_leg(leg, &mut arrays, &self.memory))?;
        let schema = Arc::new(Schema::new(vec![Field::new("tx-ops", union.data_type().clone(), false)]));
        let batch = RecordBatch::try_new(schema, vec![union]).map_err(|error| invalid(error.to_string()))?;
        arrays.finish(batch)
    }
}

fn decode_placed_seed(tx: &rtdb_format::generated::xtdb::log::proto::Tx) -> Result<Option<PlacedAttemptSeed>> {
    tx.placed_attempt_seed.as_deref().map(PlacedAttemptSeed::decode).transpose()
}

/// The scratch one operation's body takes while it is encoded, beside its documents, arguments or IDs.
const OPERATION_BODY_BYTES: usize = 4_096;
/// The cells one operation adds to the buffers that the encoded batch builds outside its columns.
const OPERATION_CELL_BYTES: usize = 256;

struct SourceOperation<'a> {
    body: StructFields<Value>,
    iids: Cow<'a, [Iid]>,
    documents: Option<(String, SourceDocuments<'a>)>,
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum SourcePurpose { Log, Receipt }

enum SourceDocuments<'a> {
    Rows(&'a Documents),
    Arrow(&'a ArrowDocuments),
    Receipt { documents: &'a ArrowDocuments, omit_nulls: bool },
}

impl SourceDocuments<'_> {
    fn columns(&self) -> &[String] {
        match self { Self::Rows(documents) => documents.columns(), Self::Arrow(documents) | Self::Receipt { documents, .. } => documents.columns() }
    }
    fn len(&self) -> usize {
        match self { Self::Rows(documents) => documents.rows().len(), Self::Arrow(documents) | Self::Receipt { documents, .. } => documents.array().len() }
    }
}

fn owned_path_bytes(relation: &rtdb_types::OwnedRelation, name: &rtdb_types::OwnedRelation, columns: &[String]) -> Result<usize> {
    columns.iter().try_fold(relation.name().len() + name.name().len(), |bytes, column| {
        bytes.checked_add(column.capacity()).and_then(|bytes| bytes.checked_add(64)).ok_or_else(overflow)
    })
}

// A shared document layout can backfill both omitted fields and explicit
// nulls with the same Arrow null cell.
fn owned_field_presence(documents: &Documents) -> Value {
    Value::List(documents.rows().iter().map(|row| Value::List(
        row.iter().filter(|(_, value)| !matches!(value, Value::Undefined))
            .map(|(name, _)| Value::Utf8(name.clone())).collect(),
    )).collect())
}

fn read_owned_field_presence(value: Value) -> Result<Option<Vec<std::collections::BTreeSet<String>>>> {
    let rows = match value {
        Value::Null | Value::Undefined => return Ok(None),
        Value::List(rows) => rows,
        _ => return Err(invalid("Owned field presence is not a row list")),
    };
    let rows = rows.into_iter().map(|row| {
        let Value::List(fields) = row else { return Err(invalid("Owned field presence is not a field list")); };
        let mut names = std::collections::BTreeSet::new();
        for field in fields {
            let Value::Utf8(name) = field else { return Err(invalid("Owned field presence name is not text")); };
            if !names.insert(name) { return Err(invalid("Duplicate owned field presence name")); }
        }
        Ok(names)
    }).collect::<Result<Vec<_>>>()?;
    Ok(Some(rows))
}

fn operation_value<'a>(operation: &'a Operation, memory: &Arc<MemoryBudget>, purpose: SourcePurpose) -> Result<(String, SourceOperation<'a>)> {
    let mut body = StructFields::new();
    let mut docs = None;
    let mut identities = Cow::Borrowed(&[][..]);
    let tag = match operation {
        Operation::Public(TxOp::Sql { sql, args }) => {
            body.insert("query".into(), Value::Utf8(sql.clone()));
            body.insert("args".into(), args.as_ref().map(|args| encode_arguments(args, memory).map(|bytes| Value::Binary(bytes.as_slice().to_vec()))).transpose()?.unwrap_or(Value::Null));
            "sql"
        }
        Operation::Public(TxOp::SqlBytes { sql, arg_bytes }) => {
            body.insert("query".into(), Value::Utf8(sql.clone()));
            body.insert("args".into(), if arg_bytes.as_slice().is_empty() { Value::Null } else { Value::Binary(arg_bytes.as_slice().to_vec()) });
            "sql"
        }
        Operation::Public(TxOp::OwnedCheckCapacity { relation, column, prefix, at_most }) => {
            body.insert("relation".into(), Value::Utf8(relation.name().into()));
            body.insert("column".into(), Value::Utf8(column.clone()));
            body.insert("prefix".into(), Value::Binary(prefix.clone()));
            body.insert("at_most".into(), Value::I64(*at_most));
            "rtdb-owned-check-capacity-v1"
        }
        Operation::Public(TxOp::OwnedDeclareCapacity { relation, column, prefix_bytes, cap, when }) => {
            body.insert("relation".into(), Value::Utf8(relation.name().into()));
            body.insert("column".into(), Value::Utf8(column.clone()));
            body.insert("prefix_bytes".into(), Value::I64(*prefix_bytes));
            body.insert("cap".into(), Value::I64(*cap));
            body.insert("when".into(), Value::Binary(when.clone()));
            "rtdb-owned-capacity-v2"
        }
        Operation::Public(TxOp::OwnedDeclareUniquePath { relation, name, column }) => {
            body.insert("relation".into(), Value::Utf8(relation.name().into()));
            body.insert("name".into(), Value::Utf8(name.name().into()));
            body.insert("column".into(), Value::Utf8(column.clone()));
            "rtdb-owned-unique-path-v1"
        }
        Operation::Public(TxOp::OwnedDeclareSearchPath { relation, name, column }) => {
            body.insert("relation".into(), Value::Utf8(relation.name().into()));
            body.insert("name".into(), Value::Utf8(name.name().into()));
            body.insert("column".into(), Value::Utf8(column.clone()));
            "rtdb-owned-search-path-v1"
        }
        Operation::Public(TxOp::OwnedDropSpatialPath { relation, name }) => {
            body.insert("relation".into(), Value::Utf8(relation.name().into()));
            body.insert("name".into(), Value::Utf8(name.name().into()));
            "rtdb-owned-drop-spatial-path-v1"
        }
        Operation::Public(TxOp::OwnedDeclareRelation { relation, schema }) => {
            body.insert("relation".into(), Value::Utf8(relation.name().into()));
            body.insert("schema".into(), Value::Binary(schema.to_json()?));
            "rtdb-owned-relation-v1"
        }
        Operation::Public(TxOp::OwnedDeclarePath { relation, name, columns } | TxOp::OwnedDeclareSpatialPath { relation, name, columns }) => {
            body.insert("relation".into(), Value::Utf8(relation.name().into()));
            body.insert("name".into(), Value::Utf8(name.name().into()));
            body.insert("columns".into(), Value::List(columns.iter().cloned().map(Value::Utf8).collect()));
            if matches!(operation, Operation::Public(TxOp::OwnedDeclareSpatialPath { .. })) {
                "rtdb-owned-spatial-path-v1"
            } else {
                "rtdb-owned-path-v1"
            }
        }
        Operation::Public(TxOp::OwnedDeclareFixedRange { relation, rule }) => {
            body.insert("relation".into(), Value::Utf8(relation.name().into()));
            body.insert("rule".into(), rule.to_value());
            "rtdb-owned-fixed-range-v1"
        }
        Operation::Public(TxOp::OwnedCheckRange { relation, basis, scope, frontier, until, fixed }) => {
            body.insert("relation".into(), Value::Utf8(relation.name().into()));
            body.insert("basis_id".into(), Value::I64(basis.tx_id.0));
            body.insert("basis_time".into(), Value::I64(basis.system_time.0));
            body.insert("scope".into(), Value::Binary(scope.encode().to_vec()));
            body.insert("frontier".into(), Value::I64(*frontier));
            body.insert("until".into(), Value::I64(until.0));
            if let Some(fixed) = fixed {
                body.insert("fixed".into(), fixed.to_value());
                "rtdb-owned-check-range-v2"
            } else {
                "rtdb-owned-check-range-v1"
            }
        }
        Operation::Public(TxOp::OwnedForkWorld { record, previous, sibling, overlay_source, attempt, fingerprint, root }) => {
            body.insert("registration".into(), Value::Struct(record.clone()));
            body.insert("previous".into(), previous.map_or(Value::Null, |pin| Value::Uuid(pin.0)));
            body.insert("attempt".into(), Value::Utf8(attempt.clone()));
            body.insert("fingerprint".into(), Value::Binary(fingerprint.to_vec()));
            if let Some(sibling) = sibling {
                body.insert("sibling".into(), Value::Uuid(sibling.0));
            }
            if let Some((world, pin)) = overlay_source {
                body.insert("overlay_world".into(), Value::Uuid(world.0));
                body.insert("overlay_pin".into(), Value::Uuid(pin.0));
            }
            match root {
                crate::OwnedForkRoot::None if overlay_source.is_some() => "rtdb-owned-fork-overlay-v1",
                crate::OwnedForkRoot::None => "rtdb-owned-fork-world-v2",
                crate::OwnedForkRoot::Writes => "rtdb-owned-root-retained-fork-v1",
                crate::OwnedForkRoot::WritesAdvance => "rtdb-owned-root-retained-advance-v1",
                crate::OwnedForkRoot::WritesRenewal => "rtdb-owned-root-retained-renewal-v1",
                crate::OwnedForkRoot::WritesSibling => "rtdb-owned-root-retained-sibling-v1",
                crate::OwnedForkRoot::Controls => "rtdb-owned-controls-retained-fork-v1",
                crate::OwnedForkRoot::ProposalControls => "rtdb-owned-controls-proposal-fork-v1",
                crate::OwnedForkRoot::ProposalHold => "rtdb-owned-root-proposal-hold-v1",
                crate::OwnedForkRoot::ProposalShorten => "rtdb-owned-root-proposal-shorten-v1",
                crate::OwnedForkRoot::ProposalWrites => "rtdb-owned-root-proposal-fork-v1",
            }
        }
        Operation::Public(TxOp::OwnedPublishRoot { attempt, fingerprint }) => {
            body.insert("attempt".into(), Value::Utf8(attempt.clone()));
            body.insert("fingerprint".into(), Value::Binary(fingerprint.to_vec()));
            "rtdb-owned-publish-root-v1"
        }
        Operation::Public(TxOp::OwnedPublishWorld { world, parent_pin, attempt, fingerprint, expected, advance_to, drop_world, settlement_pin, land_root, delete_root }) => {
            body.insert("world".into(), Value::Uuid(world.0));
            body.insert("parent_pin".into(), Value::Uuid(parent_pin.0));
            body.insert("attempt".into(), Value::Utf8(attempt.clone()));
            body.insert("fingerprint".into(), Value::Binary(fingerprint.to_vec()));
            body.insert("expected".into(), expected.map_or(Value::Null, |time| Value::I64(time.0)));
            if let Some(pin) = settlement_pin {
                body.insert("pin".into(), Value::Uuid(pin.0));
                "rtdb-owned-settle-proposal-v1"
            } else if let Some(pin) = land_root {
                body.insert("pin".into(), Value::Uuid(pin.0));
                "rtdb-owned-land-proposal-v1"
            } else if let Some(pin) = delete_root {
                body.insert("pin".into(), Value::Uuid(pin.0));
                "rtdb-owned-drop-with-root-delete-v1"
            } else if *drop_world {
                "rtdb-owned-drop-world-v1"
            } else if let Some(to) = advance_to {
                body.insert("advance_to".into(), Value::I64(to.0));
                "rtdb-owned-publish-world-v2"
            } else {
                "rtdb-owned-publish-world-v1"
            }
        }
        Operation::Public(TxOp::OwnedCheckWorldPurge { relation, pin, frontier, local_frontier }) => {
            body.insert("relation".into(), Value::Utf8(relation.name().into()));
            body.insert("pin".into(), Value::Uuid(pin.0));
            body.insert("frontier".into(), Value::I64(*frontier));
            body.insert("local_frontier".into(), local_frontier.map_or(Value::Null, Value::I64));
            "rtdb-owned-check-world-purge-v3"
        }
        Operation::Public(TxOp::OwnedCheckWorldScope { scope, clock }) => {
            body.insert("scope".into(), Value::Binary(scope.encode().to_vec()));
            if let Some(clock) = clock {
                body.insert("world".into(), Value::Uuid(clock.world.0));
                body.insert("pin".into(), Value::Uuid(clock.pin.0));
                body.insert("before".into(), Value::I64(clock.before.0));
                "rtdb-owned-check-world-clock-v1"
            } else {
                "rtdb-owned-check-world-scope-v1"
            }
        }
        Operation::Public(TxOp::HostedControl(control)) => {
            body = control.body();
            if control.is_check() { "rtdb-hosted-control-check-v1" } else { "rtdb-hosted-control-v1" }
        }
        Operation::Public(TxOp::HostedConstraintRefusal) => {
            body.insert("version".into(), Value::I64(1));
            "rtdb-hosted-constraint-refusal-v1"
        }
        Operation::Public(TxOp::OwnedDeadline { until, recheck_at_staging }) => {
            body.insert("until".into(), Value::I64(until.0));
            if *recheck_at_staging { "rtdb-owned-staging-deadline-v1" } else { "rtdb-owned-deadline-v1" }
        }
        Operation::Public(TxOp::OwnedCheck { relation, id, valid_at, expected }) => {
            body.insert("relation".into(), Value::Utf8(relation.name().into()));
            body.insert("id".into(), id.as_value());
            body.insert("valid_at".into(), Value::I64(valid_at.0));
            body.insert("expected".into(), expected.map_or(Value::Null, |expected| Value::I64(expected.0)));
            "rtdb-owned-check-v1"
        }
        Operation::Public(TxOp::OwnedSweepExpired { relation, id, created, expiry, scope }) => {
            body.insert("relation".into(), Value::Utf8(relation.name().into()));
            body.insert("id".into(), id.as_value());
            body.insert("created".into(), Value::I64(created.0));
            body.insert("expiry".into(), Value::I64(expiry.0));
            body.insert("scope".into(), Value::Binary(scope.encode().to_vec()));
            "rtdb-owned-sweep-expired-v2"
        }
        Operation::Public(TxOp::OwnedErase { relation, id }) => {
            body.insert("relation".into(), Value::Utf8(relation.name().into()));
            body.insert("id".into(), id.as_value());
            "rtdb-owned-erase-v2"
        }
        Operation::Public(TxOp::OwnedDelete { relation, id, valid_from, valid_to }) => {
            body.insert("relation".into(), Value::Utf8(relation.name().into()));
            body.insert("id".into(), id.as_value());
            time_fields(&mut body, *valid_from, *valid_to);
            "rtdb-owned-delete-v1"
        }
        Operation::Public(TxOp::OwnedInsertDocs { relation, documents, valid_from, valid_to } | TxOp::OwnedReplaceDocs { relation, documents, valid_from, valid_to }) => {
            let iids = documents.rows().iter().map(|document| {
                document_id(document, memory).map(|id| id.iid())
            }).collect::<Result<_>>()?;
            identities = Cow::Owned(iids);
            body.insert("iids".into(), Value::Null);
            body.insert("field_presence".into(), owned_field_presence(documents));
            time_fields(&mut body, *valid_from, *valid_to);
            docs = Some((relation.name().to_owned(), SourceDocuments::Rows(documents)));
            if matches!(operation, Operation::Public(TxOp::OwnedInsertDocs { .. })) {
                "rtdb-owned-insert-v1"
            } else {
                "rtdb-owned-replace-v1"
            }
        }
        Operation::OwnedWriteIids { create_only, relation, documents, iids, valid_from, valid_to } => {
            identities = Cow::Borrowed(iids);
            body.insert("iids".into(), Value::Null);
            if documents.has_exact_presence() {
                body.insert("field_presence".into(), owned_field_presence(documents));
            }
            time_fields(&mut body, *valid_from, *valid_to);
            docs = Some((relation.name().to_owned(), SourceDocuments::Rows(documents)));
            if *create_only { "rtdb-owned-insert-v1" } else { "rtdb-owned-replace-v1" }
        }
        Operation::Public(TxOp::InsertDocs { table, documents, valid_from, valid_to } | TxOp::PutDocs { table, documents, valid_from, valid_to } | TxOp::PatchDocs { table, documents, valid_from, valid_to }) => {
            let iids = documents.rows().iter().map(|document| {
                document_id(document, memory).map(|id| id.iid())
            }).collect::<Result<_>>()?;
            identities = Cow::Owned(iids);
            body.insert("iids".into(), Value::Null);
            time_fields(&mut body, *valid_from, *valid_to);
            docs = Some((table_name(table), SourceDocuments::Rows(documents)));
            match operation {
                Operation::Public(TxOp::InsertDocs { .. }) => "insert-docs",
                Operation::Public(TxOp::PatchDocs { .. }) => "patch-docs",
                _ => "put-docs",
            }
        }
        Operation::Public(TxOp::PutArrowDocs { table, documents }) => {
            let reader = documents.id_reader()?;
            let iids = (0..documents.array().len()).map(|row| {
                documents.iid(row, reader.as_ref(), memory)
            }).collect::<Result<_>>()?;
            identities = Cow::Owned(iids);
            body.insert("iids".into(), Value::Null);
            time_fields(&mut body, None, None);
            docs = Some((table_name(table), SourceDocuments::Arrow(documents)));
            "put-docs"
        }
        Operation::InsertArrowIids { table, documents, iids, valid_from, valid_to } => {
            identities = Cow::Borrowed(iids);
            body.insert("iids".into(), Value::Null);
            time_fields(&mut body, *valid_from, *valid_to);
            let documents = if purpose == SourcePurpose::Receipt {
                SourceDocuments::Receipt { documents, omit_nulls: true }
            } else {
                SourceDocuments::Arrow(documents)
            };
            docs = Some((table_name(table), documents));
            "insert-docs"
        }
        Operation::PutReplayArrow { table, documents, iids, valid_from, valid_to } => {
            identities = match iids {
                Some(iids) => Cow::Borrowed(iids),
                None => {
                    let reader = documents.id_reader()?;
                    Cow::Owned((0..documents.array().len()).map(|row| documents.iid(row, reader.as_ref(), memory)).collect::<Result<_>>()?)
                }
            };
            body.insert("iids".into(), Value::Null);
            time_fields(&mut body, *valid_from, *valid_to);
            let payload = if purpose == SourcePurpose::Receipt && (iids.is_none() || documents.requires_legacy_receipt(false)) {
                SourceDocuments::Receipt { documents, omit_nulls: true }
            } else {
                SourceDocuments::Arrow(documents)
            };
            docs = Some((table_name(table), payload));
            "put-docs"
        }
        Operation::PatchArrowIids { table, documents, iids, valid_from, valid_to } => {
            identities = Cow::Borrowed(iids);
            body.insert("iids".into(), Value::Null);
            time_fields(&mut body, *valid_from, *valid_to);
            let payload = if purpose == SourcePurpose::Receipt && documents.requires_legacy_receipt(true) {
                SourceDocuments::Receipt { documents, omit_nulls: false }
            } else {
                SourceDocuments::Arrow(documents)
            };
            docs = Some((table_name(table), payload));
            "patch-docs"
        }
        Operation::InsertIids { table, documents, iids, valid_from, valid_to } | Operation::PutIids { table, documents, iids, valid_from, valid_to } | Operation::PatchIids { table, documents, iids, valid_from, valid_to } => {
            identities = Cow::Borrowed(iids);
            body.insert("iids".into(), Value::Null);
            time_fields(&mut body, *valid_from, *valid_to);
            docs = Some((table_name(table), SourceDocuments::Rows(documents)));
            match operation {
                Operation::InsertIids { .. } => "insert-docs",
                Operation::PatchIids { .. } => "patch-docs",
                _ => "put-docs",
            }
        }
        Operation::Public(TxOp::DeleteDocs { table, ids, valid_from, valid_to }) => {
            body.insert("table".into(), Value::Utf8(table_name(table)));
            identities = Cow::Owned(ids.ids().iter().map(EntityId::iid).collect());
            body.insert("iids".into(), Value::Null);
            time_fields(&mut body, *valid_from, *valid_to);
            "delete-docs"
        }
        Operation::Public(TxOp::EraseDocs { table, ids }) => {
            body.insert("table".into(), Value::Utf8(table_name(table)));
            identities = Cow::Owned(ids.ids().iter().map(EntityId::iid).collect());
            body.insert("iids".into(), Value::Null);
            "erase-docs"
        }
        Operation::Removed(name) => return Err(Anomaly::unsupported("rtdb.tx/removed-operation", format!("Cannot encode removed operation {name}"))),
    };
    Ok((tag.into(), SourceOperation { body, iids: identities, documents: docs }))
}

pub(crate) fn document_id(document: &StructFields<Value>, memory: &Arc<MemoryBudget>) -> Result<EntityId> {
    if let Some(id) = document.get("_id").filter(|id| !matches!(id, Value::Null | Value::Undefined)) {
        return id.entity_id();
    }
    let bound = document.iter().try_fold(4096usize, |bytes, (key, value)| {
        let key_bytes = key.len().checked_mul(8).ok_or_else(overflow)?;
        let value_bytes = rtdb_expr::text_allocation_bound(value)?;
        bytes.checked_add(key_bytes).and_then(|bytes| bytes.checked_add(value_bytes)).ok_or_else(overflow)
    })?;
    let _scratch = memory.reserve(bound)?;
    let typed = rtdb_format::transit::document_to_transit(document)?;
    let _json = memory.reserve(rtdb_format::transit::json_value_allocation_bound(&typed)?)?;
    Err(Anomaly::incorrect("missing-id", "missing '_id'").with_transit_context("doc", &typed)?)
}

fn named_union<T>(
    values: impl IntoIterator<Item = (String, T)>,
    mut build: impl FnMut(Vec<T>) -> Result<ArrayRef>,
) -> Result<ArrayRef> {
    let mut names = Vec::new();
    let mut legs: Vec<Vec<T>> = Vec::new();
    let mut ids = Vec::new();
    let mut offsets = Vec::new();
    for (name, value) in values {
        let leg = match names.iter().position(|existing| existing == &name) {
            Some(leg) => leg,
            None => { names.push(name); legs.push(Vec::new()); names.len() - 1 }
        };
        ids.push(i8::try_from(leg).map_err(|_| invalid("Too many transaction operation legs"))?);
        offsets.push(i32::try_from(legs[leg].len()).map_err(|_| overflow())?);
        legs[leg].push(value);
    }
    let mut fields = Vec::new();
    let mut children = Vec::new();
    for (name, values) in names.into_iter().zip(legs) {
        let array = build(values)?;
        fields.push(Arc::new(Field::new(name, array.data_type().clone(), false)));
        children.push(array);
    }
    Ok(Arc::new(UnionArray::try_new(UnionFields::new((0..fields.len()).map(|id| id as i8), fields), ScalarBuffer::from(ids), Some(ScalarBuffer::from(offsets)), children).map_err(|error| invalid(error.to_string()))?))
}

fn operation_leg(values: Vec<SourceOperation<'_>>, arrays: &mut BatchBuilder, memory: &Arc<MemoryBudget>) -> Result<ArrayRef> {
    let mut iids = Vec::with_capacity(values.len());
    let (values, documents): (Vec<_>, Vec<_>) = values.into_iter().map(|value| {
        iids.push(value.iids);
        (value.body, value.documents)
    }).unzip();
    let types = values.iter().map(|value| LogicalType::Struct(value.iter().map(|(name, value)| (name.clone(), value.logical_type())).collect())).collect::<Vec<_>>();
    let LogicalType::Struct(mut types) = LogicalType::join_all(&types) else { return Err(invalid("Transaction operation leg has no struct layout")); };
    for temporal in ["_valid_from", "_valid_to"] {
        if let Some(ty) = types.get_mut(temporal) {
            *ty = LogicalType::Scalar(ScalarType::Timestamp { unit: TimeUnit::Microsecond, timezone: Some("UTC".into()) }).maybe();
        }
    }
    if let Some(ty) = types.get_mut("iids") { *ty = LogicalType::list(LogicalType::Scalar(ScalarType::FixedSizeBinary(16))); }
    // The reference's argument vector is non-nullable until an operation without arguments writes it a null.
    if let Some(ty) = types.get_mut("args") {
        let binary = LogicalType::Scalar(ScalarType::Binary);
        *ty = if *ty == binary { binary } else { binary.maybe() };
    }
    let mut fields = Vec::new();
    let mut columns = Vec::new();
    // The reference's document writers declare `documents` straight after `iids`, ahead of the valid-time columns.
    let mut documents = documents.iter().any(Option::is_some).then_some(documents);
    for (name, ty) in types {
        let field = Arc::new(to_field(&name, &ty)?);
        let column = if name == "iids" {
            iid_vector(&iids, &field, arrays)?
        } else {
            let cells = || values.iter().map(|value| struct_field_cell(value, &name, &ty));
            let bound = estimate_builder_bytes_refs(&ty, cells())?;
            arrays.column(bound, || copy_array(&ty, &cells().cloned().collect::<Vec<_>>()))?
        };
        columns.push(column);
        fields.push(field);
        if name == "iids" {
            if let Some(documents) = documents.take() {
                let documents = named_union(documents.into_iter().flatten(), |leg| document_leg(leg, arrays, memory))?;
                fields.push(Arc::new(Field::new("documents", documents.data_type().clone(), false)));
                columns.push(documents);
            }
        }
    }
    if documents.is_some() { return Err(invalid("Document operation has no iids")); }
    rtdb_columnar::build_struct_array(fields.into(), &columns, values.len(), None).map_err(|error| invalid(error.to_string()))
}

fn iid_vector(lists: &[Cow<'_, [Iid]>], field: &Field, arrays: &mut BatchBuilder) -> Result<ArrayRef> {
    let DataType::List(item) = field.data_type() else { return Err(invalid("IIDs must be a list")); };
    let count = lists.iter().try_fold(0usize, |count, list| count.checked_add(list.len()).ok_or_else(overflow))?;
    i32::try_from(count).map_err(|_| overflow())?;
    let bytes = count.checked_mul(16).ok_or_else(overflow)?;
    let bound = bytes.checked_add(lists.len().checked_mul(16).ok_or_else(overflow)?)
        .and_then(|bytes| bytes.checked_add(4096)).ok_or_else(overflow)?;
    arrays.column(bound, || {
        let mut values = Vec::with_capacity(bytes);
        let mut offsets = Vec::with_capacity(lists.len() + 1);
        offsets.push(0);
        let mut end = 0i32;
        for list in lists {
            for iid in list.as_ref() { values.extend_from_slice(&iid.0); }
            end += list.len() as i32;
            offsets.push(end);
        }
        let values = Arc::new(FixedSizeBinaryArray::new(16, Buffer::from_vec(values), None));
        let array = ListArray::try_new(Arc::clone(item), OffsetBuffer::new(offsets.into()), values, None)
            .map_err(|error| invalid(error.to_string()))?;
        Ok(Arc::new(array))
    })
}

fn document_leg(lists: Vec<SourceDocuments<'_>>, arrays: &mut BatchBuilder, memory: &Arc<MemoryBudget>) -> Result<ArrayRef> {
    let arrow = lists.iter().map(|list| match list {
        SourceDocuments::Arrow(documents) => Some(documents.array()),
        SourceDocuments::Rows(_) | SourceDocuments::Receipt { .. } => None,
    }).collect::<Option<Vec<_>>>();
    if let Some(pages) = arrow
        && let Some(leg) = arrow_document_leg(&pages, arrays)? {
        return Ok(leg);
    }
    if lists.iter().all(|list| !matches!(list, SourceDocuments::Receipt { .. }))
        && lists.iter().any(|list| matches!(list, SourceDocuments::Arrow(_)))
        && let Some(leg) = mixed_document_leg(&lists, arrays)? {
        return Ok(leg);
    }
    let offsets = document_offsets(lists.iter().map(SourceDocuments::len))?;
    let structs = canonical_document_struct(&lists, *offsets.last().expect("document origin") as usize, arrays, memory)?;
    document_list(structs, offsets)
}

fn mixed_document_leg(lists: &[SourceDocuments<'_>], arrays: &mut BatchBuilder) -> Result<Option<ArrayRef>> {
    let offsets = document_offsets(lists.iter().map(SourceDocuments::len))?;
    let length = *offsets.last().unwrap() as usize;
    let names = lists.iter().flat_map(SourceDocuments::columns).collect::<OrderedSet<_>>();
    let mut layout = Vec::with_capacity(names.len());
    for name in names {
        let mut ty = LogicalType::Nothing;
        let mut present = 0;
        for list in lists {
            match list {
                SourceDocuments::Arrow(documents) | SourceDocuments::Receipt { documents, .. } => {
                    if let Some((field, _)) = document_column(documents.array(), name) {
                        let Some(actual) = document_relayout_type(field)? else { return Ok(None); };
                        if !canonical_union_layouts(field) { return Ok(None); }
                        if ty != actual { ty = ty.join(&actual); }
                        present += list.len();
                    }
                }
                SourceDocuments::Rows(documents) => {
                    if let Some(declared) = documents.declared_types().and_then(|columns| columns.get(name)) {
                        if &ty != declared { ty = ty.join(declared); }
                    }
                    for value in documents.rows().iter().filter_map(|row| row.get(name)) {
                        let actual = value.logical_type();
                        if ty != actual { ty = ty.join(&actual); }
                        present += 1;
                    }
                }
            }
        }
        if present < length || present == 0 { ty = ty.maybe(); }
        let depth = struct_backfill_depth(&ty);
        if depth != 0 {
            let scratch = length.checked_mul(depth + 1).and_then(|cells| cells.checked_mul(std::mem::size_of::<WriterCell<'_>>())).ok_or_else(overflow)?;
            let _cells = arrays.reserve_scratch(scratch)?;
            let mut cells = Vec::with_capacity(length);
            cells.extend(lists.iter().flat_map(|list| {
                let column = match list {
                    SourceDocuments::Arrow(documents) | SourceDocuments::Receipt { documents, .. } => document_column(documents.array(), name),
                    SourceDocuments::Rows(_) => None,
                };
                (0..list.len()).map(move |row| match list {
                    SourceDocuments::Rows(documents) => documents.rows()[row].get(name).map_or(WriterCell::Null, WriterCell::of),
                    SourceDocuments::Arrow(_) | SourceDocuments::Receipt { .. } => column.map_or(WriterCell::Null, |(field, array)| WriterCell::arrow(field, array, row)),
                })
            }));
            ty = backfilled_nullability(ty, &cells);
        }
        layout.push((to_field(name, &ty)?, ty));
    }

    let mut columns = Vec::with_capacity(layout.len());
    let mut fields = Vec::with_capacity(layout.len());
    for (field, ty) in layout {
        let bound = lists.iter().try_fold(4096usize, |bytes, list| {
            let size = match list {
                SourceDocuments::Rows(documents) => {
                    let values = documents.rows().iter().map(|row| struct_field_cell(row, field.name(), &ty));
                    estimate_builder_bytes_refs(&ty, values)?
                }
                SourceDocuments::Arrow(documents) | SourceDocuments::Receipt { documents, .. } => document_column_bound(documents.array(), &field, &ty)?,
            };
            bytes.checked_add(size).ok_or_else(overflow)
        })?;
        columns.push(write_document_column(arrays, &field, bound, |selection, scratch| {
            lists.iter().map(|list| match list {
                SourceDocuments::Rows(documents) => {
                    let values = documents.rows().iter().map(|row| struct_field_cell(row, field.name(), &ty).clone()).collect::<Vec<_>>();
                    copy_array(&ty, &values)
                }
                SourceDocuments::Arrow(documents) | SourceDocuments::Receipt { documents, .. } => align_document_column(documents.array(), &field, selection, scratch),
            }).collect()
        })?);
        fields.push(Arc::new(field));
    }
    let structs = rtdb_columnar::build_struct_array(fields.into(), &columns, length, None)?;
    document_list(structs, offsets).map(Some)
}

fn canonical_union_layouts(field: &Field) -> bool {
    match field.data_type() {
        DataType::Union(fields, UnionMode::Dense) => {
            let mut names = OrderedSet::with_capacity(fields.len());
            from_field(field).is_ok_and(|ty| matches!(ty, LogicalType::Union(_)))
                && fields.iter().all(|(_, field)| {
                    canonical_union_layouts(field) && from_field(field)
                        .and_then(|ty| rtdb_columnar::leg_name(&ty)).is_ok_and(|name| names.insert(name))
                })
        }
        DataType::Struct(fields) => fields.iter().all(|field| canonical_union_layouts(field)),
        DataType::List(field) | DataType::FixedSizeList(field, _) => canonical_union_layouts(field),
        _ => true,
    }
}

fn arrow_document_leg(pages: &[&StructArray], arrays: &mut BatchBuilder) -> Result<Option<ArrayRef>> {
    let Some(first) = pages.first() else { return Ok(None); };
    let offsets = document_offsets(pages.iter().map(|page| page.len()))?;
    if pages.len() == 1 {
        let bound = first.get_array_memory_size().checked_add(4096).ok_or_else(overflow)?;
        let structs = arrays.column(bound, || Ok(Arc::new((*first).clone())))?;
        return document_list(structs, offsets).map(Some);
    }

    let mut layout = StructFields::<Field>::new();
    for page in pages {
        for field in page.fields() {
            let Some(previous) = layout.get(field.name()) else {
                layout.insert(field.name().clone(), field.as_ref().clone());
                continue;
            };
            let merged = if previous.data_type() == field.data_type() && previous.metadata() == field.metadata() {
                previous.clone().with_nullable(previous.is_nullable() || field.is_nullable())
            } else if previous.data_type() == &DataType::Null && previous.is_nullable() {
                field.as_ref().clone().with_nullable(true)
            } else if field.data_type() == &DataType::Null && field.is_nullable() {
                previous.clone().with_nullable(true)
            } else if let Some(merged) = merge_document_fields(previous, field)? {
                merged
            } else {
                return Ok(None);
            };
            layout.insert(field.name().clone(), merged);
        }
    }
    let mut output_layout = Vec::with_capacity(layout.len());
    for (name, field) in layout {
        let nullable = field.is_nullable() || pages.iter().any(|page| page.column_by_name(&name).is_none());
        let field = if nullable && matches!(field.data_type(), DataType::Union(..)) {
            let Some(logical) = document_relayout_type(&field)? else { return Ok(None) };
            to_field(name, &logical.maybe())?
        } else {
            field.with_nullable(nullable)
        };
        if pages.iter().any(|page| document_column(page, field.name()).is_some_and(|(source, _)| {
            source.data_type() != field.data_type() && !canonical_union_layouts(source)
        })) {
            return Ok(None);
        }
        output_layout.push(field);
    }
    let mut fields = Vec::with_capacity(output_layout.len());
    let mut columns = Vec::with_capacity(output_layout.len());
    for field in output_layout {
        let logical = from_field(&field)?;
        let bound = pages.iter().try_fold(4096usize, |bytes, page| {
            bytes.checked_add(document_column_bound(page, &field, &logical)?).ok_or_else(overflow)
        })?;
        columns.push(write_document_column(arrays, &field, bound, |selection, scratch| {
            pages.iter().map(|page| align_document_column(page, &field, selection, scratch)).collect()
        })?);
        fields.push(Arc::new(field));
    }
    let structs = rtdb_columnar::build_struct_array(fields.into(), &columns, *offsets.last().unwrap() as usize, None)?;
    document_list(structs, offsets).map(Some)
}

fn document_column<'a>(page: &'a StructArray, name: &str) -> Option<(&'a Field, &'a ArrayRef)> {
    page.fields().iter().zip(page.columns()).find(|(field, _)| field.name() == name).map(|(field, array)| (field.as_ref(), array))
}

fn align_document_column(page: &StructArray, field: &Field, selection: Option<&BatchBuilder>, scratch: &mut Vec<Arc<MemoryReservation>>) -> Result<ArrayRef> {
    let union = matches!(field.data_type(), DataType::Union(..));
    match document_column(page, field.name()) {
        Some((source, column)) if source.data_type() == field.data_type() => Ok(Arc::clone(column)),
        Some((source, column)) if selection.is_some() && source.data_type() != &DataType::Null => {
            let selection = selection.expect("selected source layout has an allocation budget");
            let base = source.size().checked_mul(8).and_then(|bytes| bytes.checked_add(4096)).ok_or_else(overflow)?;
            let bound = (0..column.len()).try_fold(base, |bytes, row| {
                rtdb_columnar::estimate_copy_cell_bytes(source, column.as_ref(), row)?.checked_mul(2)
                    .and_then(|cell| bytes.checked_add(cell)).ok_or_else(overflow)
            })?;
            scratch.push(selection.reserve_scratch(bound)?);
            let rows = (0..column.len()).map(|row| (0, row)).collect::<Vec<_>>();
            let column = rtdb_columnar::copy_selected_cells(&[column.as_ref()], &rows)?;
            scratch.push(selection.reserve_scratch(rtdb_columnar::arrow_relayout_bound(column.as_ref(), source, field)?)?);
            rtdb_columnar::relayout_arrow_column(column, source, field).map(|(_, column)| column)
        }
        Some((source, column)) if source.data_type() != &DataType::Null || union => {
            rtdb_columnar::relayout_arrow_column(Arc::clone(column), source, field).map(|(_, column)| column)
        }
        None if union => {
            let null = Field::new(field.name(), DataType::Null, true);
            let array = Arc::new(arrow_array::NullArray::new(page.len()));
            rtdb_columnar::relayout_arrow_column(array, &null, field).map(|(_, column)| column)
        }
        _ => Ok(arrow_array::new_null_array(field.data_type(), page.len())),
    }
}

fn document_column_bound(page: &StructArray, field: &Field, ty: &LogicalType) -> Result<usize> {
    let selected = contains_fixed_size_list(field.data_type());
    let bytes = match document_column(page, field.name()) {
        Some((source, _)) if selected && (source.data_type() != &DataType::Null || source.data_type() == field.data_type()) => 0,
        Some((source, column)) if source.data_type() == field.data_type() => column.get_array_memory_size(),
        Some((source, column)) if source.data_type() != &DataType::Null || matches!(field.data_type(), DataType::Union(..)) => {
            column.get_array_memory_size()
                .checked_add(rtdb_columnar::arrow_relayout_bound(column.as_ref(), source, field)?).ok_or_else(overflow)?
        }
        _ => estimate_builder_bytes_refs(ty, std::iter::repeat_n(&Value::Null, page.len()))?,
    };
    bytes.checked_mul(4).ok_or_else(overflow)
}

fn write_document_column(
    arrays: &mut BatchBuilder, field: &Field, alignment_bound: usize,
    prepare: impl FnOnce(Option<&BatchBuilder>, &mut Vec<Arc<MemoryReservation>>) -> Result<Vec<ArrayRef>>,
) -> Result<ArrayRef> {
    if !contains_fixed_size_list(field.data_type()) {
        return arrays.column(alignment_bound, || {
            let columns = prepare(None, &mut Vec::new())?;
            let inputs = columns.iter().map(|array| array.as_ref()).collect::<Vec<_>>();
            arrow_select::concat::concat(&inputs).map_err(|error| invalid(error.to_string()))
        });
    }
    let _aligned = arrays.reserve_scratch(alignment_bound)?;
    let mut scratch = Vec::new();
    let columns = prepare(Some(arrays), &mut scratch)?;
    let rows = columns.iter().try_fold(0usize, |count, array| count.checked_add(array.len()).ok_or_else(overflow))?;
    let bound = columns.iter().try_fold(4096usize, |bytes, array| {
        (0..array.len()).try_fold(bytes, |bytes, row| {
            rtdb_columnar::estimate_event_copy_cell_bytes(field, array.as_ref(), row)?.checked_mul(2)
                .and_then(|cell| bytes.checked_add(cell)).ok_or_else(overflow)
        })
    })?;
    arrays.column(bound, || {
        let inputs = columns.iter().map(|array| array.as_ref()).collect::<Vec<_>>();
        let mut cells = Vec::with_capacity(rows);
        cells.extend(inputs.iter().enumerate().flat_map(|(source, array)| (0..array.len()).map(move |row| Some((source, row)))));
        rtdb_columnar::copy_event_cells(&inputs, &cells)
    })
}

fn merge_document_fields(left: &Field, right: &Field) -> Result<Option<Field>> {
    let (Some(left), Some(right_type)) = (document_relayout_type(left)?, document_relayout_type(right)?) else {
        return Ok(None);
    };
    let joint = left.join(&right_type);
    to_field(right.name(), &joint).map(Some)
}

fn document_relayout_type(field: &Field) -> Result<Option<LogicalType>> {
    fn supported_field(field: &Field) -> bool {
        crate::values::compatible_document_metadata(field) && match field.data_type() {
            DataType::Struct(fields) => fields.iter().all(|field| supported_field(field)),
            DataType::List(field) | DataType::FixedSizeList(field, _) | DataType::Map(field, _)
            | DataType::RunEndEncoded(_, field) => supported_field(field),
            DataType::Union(fields, UnionMode::Dense) => fields.iter().all(|(_, field)| {
                supported_field(field) && (!field.is_nullable() || field.data_type() == &DataType::Null)
            }),
            _ => true,
        }
    }
    if !supported_field(field) { return Ok(None); }
    from_field(field).map(Some)
}

pub(crate) fn owned_row_documents(documents: &Documents, memory: &Arc<MemoryBudget>) -> Result<ArrowDocuments> {
    let mut builder = BatchBuilder::new(4096, memory)?;
    let array = canonical_document_struct(&[SourceDocuments::Rows(documents)], documents.rows().len(), &mut builder, memory)?;
    own_documents(array, builder, memory)
}

pub(crate) fn coalesce_arrow_documents(mut pages: Vec<ArrowDocuments>, memory: &Arc<MemoryBudget>) -> Result<ArrowDocuments> {
    if pages.len() == 1 { return Ok(pages.pop().expect("The document group has one page")); }
    let mut builder = BatchBuilder::new(4096, memory)?;
    let documents = pages.iter().map(ArrowDocuments::array).collect::<Vec<_>>();
    let list = if let Some(array) = arrow_document_leg(&documents, &mut builder)? {
        array
    } else {
        document_leg(pages.iter().map(SourceDocuments::Arrow).collect(), &mut builder, memory)?
    };
    let array = Arc::clone(list.as_list::<i32>().values());
    own_documents(array, builder, memory)
}

fn own_documents(array: ArrayRef, builder: BatchBuilder, memory: &Arc<MemoryBudget>) -> Result<ArrowDocuments> {
    let batch = builder.finish(RecordBatch::from(array.as_struct().clone()))?;
    let array = StructArray::from(batch.batch().clone());
    let layout = DecodedDocumentLayout::new(array.fields(), memory)?;
    ArrowDocuments::from_source(&array, layout, batch.batch().get_array_memory_size(), memory)?
        .ok_or_else(|| invalid("Document array contains a null row"))
}

enum DocumentCell<'a> {
    Value(&'a Value),
    Arrow { field: &'a Field, column: &'a dyn Array, logical: &'a LogicalType, row: usize },
}

impl DocumentCell<'_> {
    fn value(&self) -> Result<Cow<'_, Value>> {
        Ok(match self {
            Self::Value(value) => Cow::Borrowed(value),
            Self::Arrow { logical, .. } if **logical == LogicalType::Nothing => Cow::Owned(Value::Undefined),
            Self::Arrow { field, column, logical, row } => {
                let mut value = rtdb_columnar::read_value_as(logical, field, *column, *row)?;
                value.admit_object_dates();
                Cow::Owned(value)
            }
        })
    }

    fn bound(&self) -> Result<usize> {
        match self {
            Self::Value(value) => value.retained_bytes(),
            Self::Arrow { logical, .. } if **logical == LogicalType::Nothing => Ok(0),
            Self::Arrow { field, column, row, .. } => rtdb_columnar::estimate_read_cell_bytes(field, *column, *row),
        }
    }

    fn writer(&self) -> WriterCell<'_> {
        match self {
            Self::Value(value) => WriterCell::of(value),
            Self::Arrow { field, column, row, .. } => WriterCell::arrow(field, *column, *row),
        }
    }
}

fn canonical_document_struct(
    lists: &[SourceDocuments<'_>], length: usize, arrays: &mut BatchBuilder, memory: &Arc<MemoryBudget>,
) -> Result<ArrayRef> {
    let names = lists.iter().flat_map(SourceDocuments::columns).collect::<OrderedSet<_>>();
    let inventory = length.checked_mul(std::mem::size_of::<Option<DocumentCell<'_>>>() + std::mem::size_of::<WriterCell<'_>>())
        .and_then(|bytes| bytes.checked_add(4096)).ok_or_else(overflow)?;
    let _inventory = memory.reserve(inventory)?;
    let mut columns = Vec::with_capacity(names.len());
    let mut schema = Vec::with_capacity(names.len());
    for name in names {
        let mut ty = LogicalType::Nothing;
        let mut present = 0usize;
        let mut cells = Vec::with_capacity(length);
        for list in lists {
            let start = cells.len();
            match list {
                SourceDocuments::Rows(documents) => {
                    if let Some(declared) = documents.declared_types().and_then(|columns| columns.get(name))
                        && ty != *declared {
                        ty = ty.join(declared);
                    }
                    cells.extend(documents.rows().iter().map(|row| row.get(name).map(DocumentCell::Value)));
                }
                SourceDocuments::Arrow(documents) | SourceDocuments::Receipt { documents, .. } => {
                    if let Some((field, array)) = document_column(documents.array(), name) {
                        let declared = documents.column_type(name).ok_or_else(|| invalid("Document column has no logical type"))?;
                        if &ty != declared { ty = ty.join(declared); }
                        let preserve_null = !matches!(list, SourceDocuments::Receipt { omit_nulls: true, .. }) || name == "_id";
                        cells.extend((0..array.len()).map(|row| {
                            if !preserve_null && !matches!(declared, LogicalType::Nothing)
                                && matches!(WriterCell::arrow(field, array.as_ref(), row), WriterCell::Null) { return None; }
                            Some(DocumentCell::Arrow { field, column: array.as_ref(), logical: declared, row })
                        }));
                    } else {
                        cells.extend(std::iter::repeat_with(|| None).take(list.len()));
                    }
                }
            }
            for cell in cells[start..].iter().flatten() {
                let _scratch = memory.reserve(cell.bound()?)?;
                let actual = cell.value()?.logical_type();
                if ty != actual { ty = ty.join(&actual); }
                present = present.checked_add(1).ok_or_else(overflow)?;
            }
        }
        if present < length || present == 0 { ty = ty.maybe(); }
        if struct_backfill_depth(&ty) != 0 {
            let writers = cells.iter().map(|cell| cell.as_ref().map_or(WriterCell::Null, DocumentCell::writer)).collect::<Vec<_>>();
            ty = backfilled_nullability(ty, &writers);
        }
        let missing = if ty == LogicalType::Nothing { &Value::Undefined } else { &Value::Null };
        let (array, _writer) = rtdb_columnar::copy_evaluated_cells(&ty, length, memory,
            |row| cells[row].as_ref().map_or(Ok(0), DocumentCell::bound),
            |row| cells[row].as_ref().map_or_else(|| Ok(missing.clone()), |cell| cell.value().map(Cow::into_owned)),
        )?;
        columns.push(arrays.column(array.get_array_memory_size(), || Ok(array))?);
        schema.push(Arc::new(to_field(name, &ty)?));
    }
    rtdb_columnar::build_struct_array(schema.into(), &columns, length, None).map_err(|error| invalid(error.to_string()))
}

fn document_offsets(lengths: impl IntoIterator<Item = usize>) -> Result<Vec<i32>> {
    let mut offsets = vec![0i32];
    for length in lengths {
        let length = i32::try_from(length).map_err(|_| overflow())?;
        offsets.push(offsets.last().unwrap().checked_add(length).ok_or_else(overflow)?);
    }
    Ok(offsets)
}

fn document_list(structs: ArrayRef, offsets: Vec<i32>) -> Result<ArrayRef> {
    let field = Arc::new(Field::new("$data$", structs.data_type().clone(), false));
    Ok(Arc::new(ListArray::try_new(field, OffsetBuffer::new(ScalarBuffer::from(offsets)), structs, None).map_err(|error| invalid(error.to_string()))?))
}

/// One row of a document column as the reference's struct writers see it.
#[derive(Clone, Copy)]
enum WriterCell<'a> { Undefined, Null, Value(&'a Value), ArrowStruct(&'a StructArray, usize), ArrowScalar }

impl<'a> WriterCell<'a> {
    fn of(value: &'a Value) -> Self { match value { Value::Null => Self::Null, Value::Undefined => Self::Undefined, value => Self::Value(value) } }
    fn arrow(field: &Field, array: &'a dyn Array, row: usize) -> Self {
        if field.data_type() == &DataType::Null {
            return if field.is_nullable() { Self::Null } else { Self::Undefined };
        }
        if let Some(union) = array.as_union_opt() {
            let id = union.type_id(row);
            let DataType::Union(legs, _) = field.data_type() else { unreachable!("Arrow union field has union legs"); };
            let leg = legs.iter().find(|(tag, _)| *tag == id).expect("Arrow union tag has a field").1;
            return Self::arrow(leg, union.child(id).as_ref(), union.value_offset(row));
        }
        if array.is_null(row) { return Self::Null; }
        array.as_struct_opt().map_or(Self::ArrowScalar, |array| Self::ArrowStruct(array, row))
    }
    fn field(&self, name: &str) -> Option<Self> {
        match self {
            Self::Value(Value::Struct(fields)) => fields.get(name).map(Self::of),
            Self::ArrowStruct(array, row) => document_column(array, name).map(|(field, array)| Self::arrow(field, array.as_ref(), *row)),
            _ => None,
        }
    }
}

fn struct_backfill_depth(ty: &LogicalType) -> usize {
    match ty {
        LogicalType::Maybe(inner) => struct_backfill_depth(inner),
        LogicalType::Struct(children) => 1 + children.values().map(struct_backfill_depth).max().unwrap_or(0),
        _ => 0,
    }
}

/// The reference's writer backfills a struct key first written at row `r` into the struct's earlier rows: undefined where
/// `isNull` holds, null otherwise, and a null makes the field nullable. A non-nullable struct's undefined rows - written
/// because its own parent was null there - do not report `isNull`, so a key first seen after them becomes nullable although
/// its rows only lacked it (xtdb #4988). Union legs start empty and so have nothing to backfill; only single-typed struct
/// chains are adjusted, and only ever towards nullable.
fn backfilled_nullability(ty: LogicalType, cells: &[WriterCell<'_>]) -> LogicalType {
    let (nullable, inner) = match ty { LogicalType::Maybe(inner) => (true, *inner), other => (false, other) };
    let LogicalType::Struct(children) = inner else { return if nullable { inner.maybe() } else { inner }; };
    if cells.iter().any(|cell| matches!(cell, WriterCell::ArrowScalar) || matches!(cell, WriterCell::Value(value) if !matches!(value, Value::Struct(_)))) {
        let inner = LogicalType::Struct(children);
        return if nullable { inner.maybe() } else { inner };
    }
    let adjusted = children.into_iter().map(|(name, child)| {
        let first = cells.iter().position(|cell| cell.field(&name).is_some());
        let child_cells = match first {
            None => return (name, child),
            Some(first) => {
                let parent_nullable = cells[..first].iter().any(|cell| matches!(cell, WriterCell::Null));
                cells.iter().enumerate().map(|(row, cell)| match (row < first, cell) {
                    (_, WriterCell::Value(Value::Struct(_)) | WriterCell::ArrowStruct(..)) => cell.field(&name).unwrap_or(WriterCell::Null),
                    (true, WriterCell::Undefined) if !parent_nullable => WriterCell::Null,
                    (_, WriterCell::Undefined | WriterCell::Null) => WriterCell::Undefined,
                    (_, WriterCell::Value(_) | WriterCell::ArrowScalar) => WriterCell::Undefined,
                }).collect::<Vec<_>>()
            }
        };
        let child = backfilled_nullability(child, &child_cells);
        let child = if child_cells.iter().any(|cell| matches!(cell, WriterCell::Null)) && !matches!(child, LogicalType::Maybe(_)) { child.maybe() } else { child };
        (name, child)
    }).collect();
    let inner = LogicalType::Struct(adjusted);
    if nullable { inner.maybe() } else { inner }
}

struct SourceOperationReader<'a> {
    memory: &'a Arc<MemoryBudget>,
    parsed_fields: &'a mut MemoryReservation,
    layouts: BTreeMap<usize, Arc<DecodedDocumentLayout>>,
}

impl SourceOperationReader<'_> {
    fn operation(&mut self, union: &UnionArray, row: usize, source_bytes: usize) -> Result<Operation> {
        let memory = self.memory;
        let parsed_fields = &mut *self.parsed_fields;
        let (name, array, offset) = union_leg(union, row)?;
        let base = parsed_fields.bytes().checked_add(name.capacity()).and_then(|bytes| bytes.checked_add(1024)).ok_or_else(overflow)?;
        if name == "xtql" || name == "call" {
            parsed_fields.grow_to(base)?;
            return Ok(Operation::Removed(name));
        }
        let body = array.as_any().downcast_ref::<StructArray>().ok_or_else(|| invalid("Transaction operation leg is not a struct"))?;
        let value_fields: &[&str] = match name.as_str() {
            "sql" => &["query", "args"],
            "rtdb-owned-search-path-v1" | "rtdb-owned-unique-path-v1" => &["relation", "name", "column"],
            "rtdb-owned-drop-spatial-path-v1" => &["relation", "name"],
            "rtdb-owned-relation-v1" => &["relation", "schema"],
            "rtdb-owned-capacity-v1" => &["relation", "column", "prefix_bytes", "cap"],
            "rtdb-owned-check-capacity-v1" => &["relation", "column", "prefix", "at_most"],
            "rtdb-owned-path-v1" | "rtdb-owned-spatial-path-v1" => &["relation", "name", "columns"],
            "rtdb-hosted-control-v1" | "rtdb-hosted-control-check-v1" => &["relation", "scope", "expected", "document"],
            "rtdb-owned-deadline-v1" | "rtdb-owned-staging-deadline-v1" => &["until"],
            "rtdb-hosted-constraint-refusal-v1" => &["version"],
            "rtdb-owned-fixed-range-v1" => &["relation", "rule"],
            "rtdb-owned-fork-overlay-v1" => &["registration", "previous", "attempt", "fingerprint", "overlay_world", "overlay_pin"],
            "rtdb-owned-root-retained-sibling-v1" => &["registration", "previous", "sibling", "attempt", "fingerprint"],
            "rtdb-owned-fork-world-v2" | "rtdb-owned-root-retained-fork-v1" | "rtdb-owned-root-retained-advance-v1" | "rtdb-owned-root-retained-renewal-v1" | "rtdb-owned-root-proposal-hold-v1" | "rtdb-owned-root-proposal-shorten-v1" | "rtdb-owned-controls-retained-fork-v1"
            | "rtdb-owned-controls-proposal-fork-v1" | "rtdb-owned-root-proposal-fork-v1" => &["registration", "previous", "attempt", "fingerprint"],
            "rtdb-owned-publish-root-v1" => &["attempt", "fingerprint"],
            "rtdb-owned-publish-world-v1" | "rtdb-owned-drop-world-v1" => &["world", "parent_pin", "attempt", "fingerprint", "expected"],
            "rtdb-owned-settle-proposal-v1" | "rtdb-owned-land-proposal-v1" | "rtdb-owned-drop-with-root-delete-v1" => &["world", "parent_pin", "attempt", "fingerprint", "expected", "pin"],
            "rtdb-owned-publish-world-v2" => &["world", "parent_pin", "attempt", "fingerprint", "expected", "advance_to"],
            "rtdb-owned-check-world-purge-v3" => &["relation", "pin", "frontier", "local_frontier"],
            "rtdb-owned-check-world-scope-v1" => &["scope"],
            "rtdb-owned-check-world-clock-v1" => &["scope", "world", "pin", "before"],
            "rtdb-owned-check-range-v1" => &["relation", "basis_id", "basis_time", "scope", "frontier", "until"],
            "rtdb-owned-check-range-v2" => &["relation", "basis_id", "basis_time", "scope", "frontier", "until", "fixed"],
            "rtdb-owned-check-v1" => &["relation", "id", "valid_at", "expected"],
            "rtdb-owned-erase-v2" => &["relation", "id"],
            "rtdb-owned-sweep-expired-v2" => &["relation", "id", "created", "expiry", "scope"],
            "rtdb-owned-delete-v1" => &["relation", "id", "_valid_from", "_valid_to"],
            "rtdb-owned-insert-v1" | "rtdb-owned-replace-v1" => &["iids", "_valid_from", "_valid_to", "field_presence"],
            "insert-docs" | "put-docs" | "patch-docs" => &["iids", "_valid_from", "_valid_to"],
            "delete-docs" => &["table", "iids", "_valid_from", "_valid_to"],
            "erase-docs" => &["table", "iids"],
            _ => &[],
        };
        let bound = body.fields().iter().zip(body.columns()).filter(|(field, _)| value_fields.contains(&field.name().as_str()))
            .try_fold(base, |bytes, (field, column)| {
                let retained = if field.name() == "iids" && let Some((_, rows)) = iid_list(field, column.as_ref(), offset) {
                    rows.len().checked_mul(std::mem::size_of::<Iid>()).ok_or_else(overflow)?
                } else {
                    estimate_read_value_bytes(field, column.as_ref(), offset)?
                };
                bytes.checked_add(retained).ok_or_else(overflow)
            })?;
        parsed_fields.grow_to(bound)?;
        let value = |name| struct_cell(body, name, offset);
        let operation = match name.as_str() {
            "rtdb-owned-fork-overlay-v1" | "rtdb-owned-root-retained-sibling-v1" | "rtdb-owned-fork-world-v2" | "rtdb-owned-root-retained-fork-v1" | "rtdb-owned-root-retained-advance-v1" | "rtdb-owned-root-retained-renewal-v1" | "rtdb-owned-root-proposal-hold-v1" | "rtdb-owned-root-proposal-shorten-v1" | "rtdb-owned-controls-retained-fork-v1"
            | "rtdb-owned-controls-proposal-fork-v1" | "rtdb-owned-root-proposal-fork-v1" => {
                let (Value::Struct(record), Value::Utf8(attempt), Value::Binary(fingerprint)) =
                    (value("registration")?, value("attempt")?, value("fingerprint")?) else {
                    return Err(invalid("Owned world fork has invalid fields"));
                };
                let fingerprint = fingerprint.try_into().map_err(|_| invalid("Invalid world fork fingerprint"))?;
                let previous = match value("previous")? {
                    Value::Uuid(pin) => Some(Iid(pin)),
                    Value::Null => None,
                    _ => return Err(invalid("Invalid world predecessor pin")),
                };
                TxOp::OwnedForkWorld {
                    record, previous, attempt, fingerprint,
                    overlay_source: if name == "rtdb-owned-fork-overlay-v1" {
                        let (Value::Uuid(world), Value::Uuid(pin)) = (value("overlay_world")?, value("overlay_pin")?) else {
                            return Err(invalid("Retained overlay has no original source and pin"));
                        };
                        Some((Iid(world), Iid(pin)))
                    } else {
                        None
                    },
                    sibling: if name == "rtdb-owned-root-retained-sibling-v1" {
                        let Value::Uuid(sibling) = value("sibling")? else { return Err(invalid("Retained fork has no sibling source")); };
                        Some(Iid(sibling))
                    } else {
                        None
                    },
                    root: match name.as_str() {
                        "rtdb-owned-root-retained-fork-v1" => crate::OwnedForkRoot::Writes,
                        "rtdb-owned-root-retained-advance-v1" => crate::OwnedForkRoot::WritesAdvance,
                        "rtdb-owned-root-proposal-hold-v1" => crate::OwnedForkRoot::ProposalHold,
                        "rtdb-owned-root-proposal-shorten-v1" => crate::OwnedForkRoot::ProposalShorten,
                        "rtdb-owned-root-retained-renewal-v1" => crate::OwnedForkRoot::WritesRenewal,
                        "rtdb-owned-root-retained-sibling-v1" => crate::OwnedForkRoot::WritesSibling,
                        "rtdb-owned-controls-retained-fork-v1" => crate::OwnedForkRoot::Controls,
                        "rtdb-owned-controls-proposal-fork-v1" => crate::OwnedForkRoot::ProposalControls,
                        "rtdb-owned-root-proposal-fork-v1" => crate::OwnedForkRoot::ProposalWrites,
                        _ => crate::OwnedForkRoot::None,
                    },
                }
            }
            "rtdb-owned-publish-root-v1" => {
                let (Value::Utf8(attempt), Value::Binary(fingerprint)) =
                    (value("attempt")?, value("fingerprint")?) else {
                    return Err(invalid("Owned Root publication has invalid fields"));
                };
                let fingerprint = fingerprint.try_into().map_err(|_| invalid("Invalid Root publication fingerprint"))?;
                TxOp::OwnedPublishRoot { attempt, fingerprint }
            }
            "rtdb-owned-settle-proposal-v1" | "rtdb-owned-land-proposal-v1" | "rtdb-owned-publish-world-v1" | "rtdb-owned-publish-world-v2" | "rtdb-owned-drop-world-v1" | "rtdb-owned-drop-with-root-delete-v1" => {
                let (Value::Uuid(world), Value::Uuid(parent_pin), Value::Utf8(attempt), Value::Binary(fingerprint)) =
                    (value("world")?, value("parent_pin")?, value("attempt")?, value("fingerprint")?) else {
                    return Err(invalid("Owned world publication has invalid fields"));
                };
                let fingerprint = fingerprint.try_into().map_err(|_| invalid("Invalid world publication fingerprint"))?;
                TxOp::OwnedPublishWorld {
                    world: rtdb_types::Iid(world), parent_pin: Iid(parent_pin), attempt, fingerprint,
                    drop_world: matches!(name.as_str(), "rtdb-owned-land-proposal-v1" | "rtdb-owned-drop-world-v1" | "rtdb-owned-drop-with-root-delete-v1"),
                    land_root: if name == "rtdb-owned-land-proposal-v1" {
                        let Value::Uuid(pin) = value("pin")? else { return Err(invalid("Proposal landing has no original pin")); };
                        Some(Iid(pin))
                    } else { None },
                    settlement_pin: if name == "rtdb-owned-settle-proposal-v1" {
                        let Value::Uuid(pin) = value("pin")? else { return Err(invalid("Proposal settlement has no original pin")); };
                        Some(Iid(pin))
                    } else { None },
                    delete_root: if name == "rtdb-owned-drop-with-root-delete-v1" {
                        let Value::Uuid(pin) = value("pin")? else { return Err(invalid("Retained Root delete has no original pin")); };
                        Some(Iid(pin))
                    } else { None },
                    advance_to: if name == "rtdb-owned-publish-world-v2" {
                        let Value::I64(to) = value("advance_to")? else {
                            return Err(invalid("Invalid world clock advance target"));
                        };
                        Some(Timestamp(to))
                    } else { None },
                    expected: match value("expected")? {
                        Value::Null | Value::Undefined => None,
                        Value::I64(time) => Some(Timestamp(time)),
                        _ => return Err(invalid("Invalid expected world application clock")),
                    },
                }
            }
            "rtdb-owned-check-world-purge-v3" => {
                let (Value::Utf8(relation), Value::Uuid(pin), Value::I64(frontier)) =
                    (value("relation")?, value("pin")?, value("frontier")?) else {
                    return Err(invalid("Owned world purge witness has invalid fields"));
                };
                let local_frontier = match value("local_frontier")? {
                    Value::Null => None,
                    Value::I64(frontier) if frontier >= 0 => Some(frontier),
                    _ => return Err(invalid("Invalid owned world overlay frontier")),
                };
                TxOp::OwnedCheckWorldPurge { relation: rtdb_types::OwnedRelation::from_source(relation)?, pin: Iid(pin), frontier, local_frontier }
            }
            "rtdb-owned-check-capacity-v1" => {
                let (Value::Utf8(relation), Value::Utf8(column), Value::Binary(prefix), Value::I64(at_most)) =
                    (value("relation")?, value("column")?, value("prefix")?, value("at_most")?)
                else { return Err(invalid("Owned capacity condition has invalid fields")); };
                TxOp::OwnedCheckCapacity { relation: rtdb_types::OwnedRelation::new(relation)?, column, prefix, at_most }
            }
            "rtdb-owned-capacity-v2" => {
                let (Value::Utf8(relation), Value::Utf8(column), Value::I64(prefix_bytes), Value::I64(cap), Value::Binary(when)) =
                    (value("relation")?, value("column")?, value("prefix_bytes")?, value("cap")?, value("when")?)
                else { return Err(invalid("Owned capacity declaration has invalid fields")); };
                TxOp::OwnedDeclareCapacity { relation: rtdb_types::OwnedRelation::new(relation)?, column, prefix_bytes, cap, when }
            }
            "rtdb-owned-unique-path-v1" => {
                let (Value::Utf8(relation), Value::Utf8(name), Value::Utf8(column)) =
                    (value("relation")?, value("name")?, value("column")?)
                else { return Err(invalid("Owned unique path declaration has invalid fields")); };
                TxOp::OwnedDeclareUniquePath { relation: rtdb_types::OwnedRelation::new(relation)?, name: rtdb_types::OwnedRelation::new(name)?, column }
            }
            "rtdb-owned-search-path-v1" => {
                let (Value::Utf8(relation), Value::Utf8(name), Value::Utf8(column)) =
                    (value("relation")?, value("name")?, value("column")?)
                else { return Err(invalid("Owned search path declaration has invalid fields")); };
                TxOp::OwnedDeclareSearchPath { relation: rtdb_types::OwnedRelation::new(relation)?, name: rtdb_types::OwnedRelation::new(name)?, column }
            }
            "rtdb-owned-drop-spatial-path-v1" => {
                let (Value::Utf8(relation), Value::Utf8(name)) = (value("relation")?, value("name")?)
                    else { return Err(invalid("Owned spatial path removal has invalid fields")); };
                TxOp::OwnedDropSpatialPath { relation: rtdb_types::OwnedRelation::new(relation)?, name: rtdb_types::OwnedRelation::new(name)? }
            }
            "rtdb-owned-relation-v1" => {
                let (Value::Utf8(relation), Value::Binary(schema)) = (value("relation")?, value("schema")?)
                    else { return Err(invalid("Owned relation declaration has invalid fields")); };
                TxOp::OwnedDeclareRelation {
                    relation: rtdb_types::OwnedRelation::new(relation)?,
                    schema: rtdb_types::OwnedSchema::from_json(&schema)?,
                }
            }
            name_of_operation @ ("rtdb-owned-path-v1" | "rtdb-owned-spatial-path-v1") => {
                let (Value::Utf8(relation), Value::Utf8(name), Value::List(columns)) =
                    (value("relation")?, value("name")?, value("columns")?)
                else { return Err(invalid("Owned path declaration has invalid fields")); };
                let columns = columns.into_iter().map(|column| match column {
                    Value::Utf8(column) => Ok(column),
                    _ => Err(invalid("Owned path names a non-text column")),
                }).collect::<Result<_>>()?;
                let relation = rtdb_types::OwnedRelation::new(relation)?;
                let spatial = name_of_operation == "rtdb-owned-spatial-path-v1";
                let name = rtdb_types::OwnedRelation::new(name)?;
                if spatial {
                    TxOp::OwnedDeclareSpatialPath { relation, name, columns }
                } else {
                    TxOp::OwnedDeclarePath { relation, name, columns }
                }
            }
            "rtdb-owned-fixed-range-v1" => {
                let Value::Utf8(relation) = value("relation")? else { return Err(invalid("Fixed range declaration has no relation")); };
                TxOp::OwnedDeclareFixedRange {
                    relation: rtdb_types::OwnedRelation::new(relation)?,
                    rule: rtdb_types::FixedRangeRule::from_value(&value("rule")?)?,
                }
            }
            "rtdb-owned-check-range-v1" | "rtdb-owned-check-range-v2" => {
                let (Value::Utf8(relation), Value::I64(basis_id), Value::I64(basis_time),
                    Value::Binary(scope), Value::I64(frontier), Value::I64(until)) =
                    (value("relation")?, value("basis_id")?, value("basis_time")?, value("scope")?, value("frontier")?, value("until")?)
                else { return Err(invalid("Owned range witness has invalid fields")); };
                TxOp::OwnedCheckRange {
                    relation: rtdb_types::OwnedRelation::new(relation)?,
                    basis: rtdb_types::TransactionKey { tx_id: rtdb_types::TxId(basis_id), system_time: Timestamp(basis_time) },
                    scope: rtdb_types::PlacedAttemptScope::decode(&scope)?,
                    frontier, until: Timestamp(until),
                    fixed: if name == "rtdb-owned-check-range-v2" {
                        Some(Arc::new(rtdb_types::FixedRangeEvidence::from_value(&value("fixed")?)?))
                    } else { None },
                }
            }
            "rtdb-owned-check-world-scope-v1" | "rtdb-owned-check-world-clock-v1" => {
                let Value::Binary(scope) = value("scope")? else { return Err(invalid("Owned world witness has no placement scope")); };
                let clock = if name == "rtdb-owned-check-world-clock-v1" {
                    let (Value::Uuid(world), Value::Uuid(pin), Value::I64(before)) =
                        (value("world")?, value("pin")?, value("before")?) else {
                            return Err(invalid("World clock condition has no exact world, pin, or deadline"));
                        };
                    Some(rtdb_types::WorldClockLimit { world: Iid(world), pin: Iid(pin), before: Timestamp(before) })
                } else { None };
                TxOp::OwnedCheckWorldScope { scope: rtdb_types::PlacedAttemptScope::decode(&scope)?, clock }
            }
            "rtdb-hosted-control-v1" | "rtdb-hosted-control-check-v1" => {
                let (Value::Utf8(relation), Value::Binary(scope), expected, Value::Struct(document)) =
                    (value("relation")?, value("scope")?, value("expected")?, value("document")?)
                else { return Err(invalid("Hosted control has invalid fields")); };
                let expected = match expected {
                    Value::I64(time) => Some(Timestamp(time)),
                    Value::Null => None,
                    _ => return Err(invalid("Hosted control has an invalid expectation")),
                };
                let relation = rtdb_types::OwnedRelation::new(relation)?;
                let scope = rtdb_types::PlacedAttemptScope::decode(&scope)?;
                TxOp::HostedControl(if name == "rtdb-hosted-control-check-v1" {
                    crate::HostedControlWrite::check(relation, scope, expected, document)?
                } else {
                    crate::HostedControlWrite::new(relation, scope, expected, document)?
                })
            }
            "rtdb-hosted-constraint-refusal-v1" => {
                if value("version")? != Value::I64(1) {
                    return Err(invalid("Invalid hosted constraint receipt version"));
                }
                TxOp::HostedConstraintRefusal
            }
            "rtdb-owned-deadline-v1" | "rtdb-owned-staging-deadline-v1" => {
                let Value::I64(until) = value("until")? else { return Err(invalid("Owned deadline is not a microsecond timestamp")); };
                TxOp::OwnedDeadline { until: Timestamp(until), recheck_at_staging: name == "rtdb-owned-staging-deadline-v1" }
            }
            "rtdb-owned-sweep-expired-v2" => {
                let (Value::Utf8(relation), Value::I64(created), Value::I64(expiry), Value::Binary(scope)) =
                    (value("relation")?, value("created")?, value("expiry")?, value("scope")?) else {
                    return Err(invalid("Owned expiry sweep has incomplete scope"));
                };
                TxOp::OwnedSweepExpired {
                    relation: rtdb_types::OwnedRelation::new(relation)?, id: value("id")?.entity_id()?,
                    created: Timestamp(created), expiry: Timestamp(expiry),
                    scope: rtdb_types::PlacedAttemptScope::decode(&scope)?,
                }
            }
            "rtdb-owned-erase-v2" => {
                let Value::Utf8(relation) = value("relation")? else { return Err(invalid("Owned erase has no relation")); };
                TxOp::OwnedErase { relation: rtdb_types::OwnedRelation::new(relation)?, id: value("id")?.entity_id()? }
            }
            "rtdb-owned-check-v1" | "rtdb-owned-delete-v1" => {
                let Value::Utf8(relation) = value("relation")? else { return Err(invalid("Owned operation has no relation")); };
                let relation = rtdb_types::OwnedRelation::from_source(relation)?;
                let id = value("id")?.entity_id()?;
                if name == "rtdb-owned-check-v1" {
                    let Value::I64(valid_at) = value("valid_at")? else { return Err(invalid("Owned observation has no valid instant")); };
                    let expected = match value("expected")? {
                        Value::I64(time) => Some(Timestamp(time)),
                        Value::Null => None,
                        _ => return Err(invalid("Owned observation has an invalid revision")),
                    };
                    TxOp::OwnedCheck { relation, id, valid_at: Timestamp(valid_at), expected }
                } else {
                    TxOp::OwnedDelete { relation, id, valid_from: optional_timestamp(value("_valid_from")?)?, valid_to: optional_timestamp(value("_valid_to")?)? }
                }
            }
            "sql" => {
                let sql = match value("query")? { Value::Utf8(sql) => sql, _ => return Err(invalid("SQL operation has no query")) };
                let args = match value("args")? { Value::Null | Value::Undefined => None, Value::Binary(bytes) => Some(decode_arguments(&bytes, memory)?), _ => return Err(invalid("SQL arguments are not IPC bytes")) };
                TxOp::Sql { sql, args }
            }
            "rtdb-owned-insert-v1" | "rtdb-owned-replace-v1" | "insert-docs" | "put-docs" | "patch-docs" => {
                let docs_array = body.column_by_name("documents").ok_or_else(|| invalid("Document operation has no documents"))?;
                let docs_union = docs_array.as_any().downcast_ref::<UnionArray>().ok_or_else(|| invalid("Document tables are not a union"))?;
                let (table, docs_array, docs_offset) = union_leg(docs_union, offset)?;
                let owned = matches!(name.as_str(), "rtdb-owned-insert-v1" | "rtdb-owned-replace-v1").then(|| rtdb_types::OwnedRelation::from_source(table.clone())).transpose()?;
                let table = if owned.is_none() { Some(parse_table(&table)?) } else { None };
                let lists = docs_array.as_any().downcast_ref::<ListArray>().ok_or_else(|| invalid("Documents are not a list"))?;
                if lists.is_null(docs_offset) { return Err(invalid("Documents are not a list")); }
                let rows = lists.value(docs_offset);
                let structs = rows.as_any().downcast_ref::<StructArray>().ok_or_else(|| invalid("Document is not a struct"))?;
                let valid_from = optional_timestamp(value("_valid_from")?)?;
                let valid_to = optional_timestamp(value("_valid_to")?)?;
                let iids = read_iids(body, offset)?;
                if iids.as_ref().is_some_and(|iids| iids.len() != structs.len()) { return Err(invalid("Document and IID counts differ")); }
                let identity = structs.fields().as_ptr() as usize;
                let layout = match self.layouts.entry(identity) {
                    std::collections::btree_map::Entry::Occupied(layout) => Arc::clone(layout.get()),
                    std::collections::btree_map::Entry::Vacant(layout) => Arc::clone(layout.insert(DecodedDocumentLayout::new(structs.fields(), memory)?)),
                };
                if let Some(table) = table {
                    let documents = ArrowDocuments::from_source(structs, layout, source_bytes, memory)?
                        .ok_or_else(|| invalid("Document is not a struct"))?;
                    return Ok(match name.as_str() {
                        "put-docs" => Operation::PutReplayArrow { table, documents, iids, valid_from, valid_to },
                        "insert-docs" => Operation::InsertArrowIids {
                            table, documents, iids: iids.ok_or_else(|| invalid("Insert operation has no IIDs"))?, valid_from, valid_to,
                        },
                        "patch-docs" => Operation::PatchArrowIids {
                            table, documents, iids: iids.ok_or_else(|| invalid("Patch operation has no IIDs"))?, valid_from, valid_to,
                        },
                        _ => unreachable!("ordinary document tag"),
                    });
                }
                if let Some(relation) = owned {
                    let presence = body.column_by_name("field_presence")
                        .map(|_| value("field_presence").and_then(read_owned_field_presence)).transpose()?.flatten();
                    let documents = Documents::read_arrow_with_presence(structs, layout, true, presence.as_deref(), memory)?;
                    let iids = iids.ok_or_else(|| invalid("Owned insert operation has no IIDs"))?;
                    return Ok(Operation::OwnedWriteIids { create_only: name == "rtdb-owned-insert-v1", relation, documents, iids, valid_from, valid_to });
                }
                return Err(invalid("Document operation has no table"));
            }
            "delete-docs" | "erase-docs" => {
                let table = match value("table")? { Value::Utf8(table) => parse_table(&table)?, _ => return Err(invalid("Delete/erase has no table")) };
                let iids = read_iids(body, offset)?.ok_or_else(|| invalid("IIDs must be a list"))?;
                let ids = EntityIds::new(iids.into_iter().map(|iid| EntityId::Uuid(iid.0)).collect(), memory)?;
                if name == "delete-docs" { TxOp::DeleteDocs { table, ids, valid_from: optional_timestamp(value("_valid_from")?)?, valid_to: optional_timestamp(value("_valid_to")?)? } }
                else { TxOp::EraseDocs { table, ids } }
            }
            _ => return Err(invalid(format!("Unknown transaction operation {name}"))),
        };
        Ok(Operation::Public(operation))
    }
}

pub(crate) fn encode_arguments(arguments: &SqlArguments, memory: &Arc<MemoryBudget>) -> Result<RetainedBytes> {
    if arguments.retains_uploaded_source_layout() && let Some(pages) = arguments.uniform_arrow_pages() {
        let first = pages[0];
        let _reservation = memory.reserve(pages.iter().try_fold(4096usize, |bytes, page| {
            page.get_array_memory_size().checked_mul(4).and_then(|size| bytes.checked_add(size)).ok_or_else(overflow)
        })?)?;
        let fields = first.schema_ref().fields().iter().enumerate().map(|(index, field)| {
            field.as_ref().clone().with_name(format!("?_{index}"))
        }).collect::<Vec<_>>();
        let schema = Arc::new(Schema::new(fields));
        let combined;
        let page = if pages.len() == 1 {
            first
        } else {
            combined = arrow_select::concat::concat_batches(first.schema_ref(), pages).map_err(|error| invalid(error.to_string()))?;
            &combined
        };
        let options = arrow_array::RecordBatchOptions::new().with_row_count(Some(page.num_rows()));
        let page = RecordBatch::try_new_with_options(Arc::clone(&schema), page.columns().to_vec(), &options)
            .map_err(|error| invalid(error.to_string()))?;
        return crate::ipc::encode(&schema, [&page], memory);
    }
    let batch = arguments.source_columnar_batch(memory)?;
    crate::ipc::encode(batch.batch().schema_ref(), [batch.batch()], memory)
}

pub(crate) fn decode_arguments(bytes: &[u8], memory: &Arc<MemoryBudget>) -> Result<SqlArguments> {
    let _value_reservation = memory.reserve(crate::ipc::allocation_bound(bytes)?)?;
    let stream = rtdb_format::ipc::OwnedStream::read(bytes, memory)?;
    let schema = Arc::clone(stream.schema());
    for (index, field) in schema.fields().iter().enumerate() {
        if !field.name().is_empty() && field.name() != &format!("${}", index + 1) && field.name() != &format!("?_{}", index) {
            return Err(Anomaly::incorrect("xtdb/invalid-param-names", "SQL argument fields must follow positional order"));
        }
    }
    let mut pages = stream.pages().iter();
    let Some(first) = pages.next() else {
        let types = schema.fields().iter().map(|field| from_field(field)).collect::<Result<Vec<_>>>()?;
        return SqlArguments::new(types, Vec::new(), memory);
    };
    if first.batch().schema() != schema { return Err(invalid("SQL argument stream changes schema")); }
    let mut arguments = SqlArguments::from_arrow(first.clone(), memory)?;
    for page in pages {
        if page.batch().schema() != schema { return Err(invalid("SQL argument stream changes schema")); }
        arguments.append_page(SqlArguments::from_arrow(page.clone(), memory)?)?;
    }
    Ok(arguments)
}

fn encode_metadata(metadata: &UserMetadata, memory: &Arc<MemoryBudget>) -> Result<RetainedBytes> {
    let ty = metadata.value().logical_type().maybe();
    let reservation = memory.reserve(estimate_builder_bytes(&ty, std::slice::from_ref(metadata.value()))?)?;
    let batch = RecordBatch::try_new(Arc::new(Schema::new(vec![to_field("user-metadata", &ty)?])), vec![build_array(&ty, std::slice::from_ref(metadata.value()))?]).map_err(|error| invalid(error.to_string()))?;
    let owned = OwnedBatch::with_reservation(batch, reservation)?;
    crate::ipc::encode(owned.batch().schema().as_ref(), [owned.batch()], memory)
}
fn decode_metadata(bytes: &[u8], memory: &Arc<MemoryBudget>) -> Result<UserMetadata> {
    let _value_reservation = memory.reserve(crate::ipc::allocation_bound(bytes)?)?;
    let pages = crate::ipc::decode(bytes, memory)?;
    if pages.len() != 1 || pages[0].batch().num_rows() != 1 { return Err(invalid("User metadata must contain one row")); }
    UserMetadata::new(cell(pages[0].batch(), "user-metadata", 0)?, memory)
}
fn union_leg(union: &UnionArray, row: usize) -> Result<(String, ArrayRef, usize)> {
    let id = union.type_id(row);
    let DataType::Union(fields, UnionMode::Dense) = union.data_type() else { return Err(invalid("Transaction union must be dense")); };
    let name = fields.iter().find_map(|(type_id, field)| (type_id == id).then(|| field.name().clone())).ok_or_else(|| invalid("Union type ID has no field"))?;
    Ok((name, Arc::clone(union.child(id)), union.value_offset(row)))
}
fn struct_cell(body: &StructArray, name: &str, row: usize) -> Result<Value> {
    let index = body.fields().iter().position(|field| field.name() == name).ok_or_else(|| invalid(format!("Operation is missing {name}")))?;
    read_value(&body.fields()[index], body.column(index).as_ref(), row)
}
fn cell(batch: &RecordBatch, name: &str, row: usize) -> Result<Value> {
    let schema = batch.schema();
    let index = schema.index_of(name).map_err(|error| invalid(error.to_string()))?;
    read_value(schema.field(index), batch.column(index).as_ref(), row)
}
fn optional_cell(batch: &RecordBatch, name: &str, row: usize) -> Result<Value> { if batch.schema().index_of(name).is_ok() { cell(batch, name, row) } else { Ok(Value::Null) } }
fn optional_timestamp(value: Value) -> Result<Option<Timestamp>> {
    match value {
        Value::Null | Value::Undefined => Ok(None),
        Value::Timestamp { value, unit: TimeUnit::Microsecond, timezone: Some(_) } => Ok(Some(Timestamp(value))),
        _ => Err(invalid("Transaction valid time must be a zoned microsecond timestamp")),
    }
}
fn optional_string(value: Value) -> Result<Option<String>> { match value { Value::Null | Value::Undefined => Ok(None), Value::Utf8(value) => Ok(Some(value)), _ => Err(invalid("Expected optional string")) } }
fn iid_list<'a>(field: &Field, array: &'a dyn Array, row: usize) -> Option<(&'a FixedSizeBinaryArray, std::ops::Range<usize>)> {
    let DataType::List(element) = field.data_type() else { return None; };
    if !field.metadata().is_empty() || !element.metadata().is_empty() || element.data_type() != &DataType::FixedSizeBinary(16) {
        return None;
    }
    let list = array.as_any().downcast_ref::<ListArray>()?;
    if list.is_null(row) { return None; }
    let values = list.values().as_any().downcast_ref::<FixedSizeBinaryArray>()?;
    let offsets = list.value_offsets();
    Some((values, offsets[row] as usize..offsets[row + 1] as usize))
}

fn read_iids(body: &StructArray, row: usize) -> Result<Option<Vec<Iid>>> {
    let index = body.fields().iter().position(|field| field.name() == "iids").ok_or_else(|| invalid("Operation is missing iids"))?;
    let field = &body.fields()[index];
    let column = body.column(index).as_ref();
    if let Some((values, rows)) = iid_list(field, column, row) {
        return rows.map(|row| {
            if values.is_null(row) { return Err(invalid("IID must be fixed-size binary")); }
            Iid::from_slice(values.value(row))
        }).collect::<Result<_>>().map(Some);
    }
    match read_value(field, column, row)? {
        Value::Null | Value::Undefined => Ok(None),
        Value::List(values) => values.into_iter().map(|value| match value {
            Value::FixedSizeBinary(bytes) => Iid::from_slice(&bytes),
            _ => Err(invalid("IID must be fixed-size binary")),
        }).collect::<Result<_>>().map(Some),
        _ => Err(invalid("IIDs must be a list")),
    }
}
fn time_fields(body: &mut StructFields<Value>, from: Option<Timestamp>, to: Option<Timestamp>) {
    let time = |time: Option<Timestamp>| time.map(|time| Value::Timestamp { value: time.0, unit: TimeUnit::Microsecond, timezone: Some("UTC".into()) }).unwrap_or(Value::Null);
    body.insert("_valid_from".into(), time(from)); body.insert("_valid_to".into(), time(to));
}
pub(crate) fn table_name(table: &TableRef) -> String { format!("{}/{}", table.schema, table.table) }
pub(crate) fn is_reserved_write_schema(schema: &str) -> bool {
    matches!(schema, "xt" | "information_schema" | "pg_catalog") || TableRef::owned_schema(schema)
}
fn check_writable_table(operation: &Operation) -> Result<()> {
    let table = match operation {
        Operation::Public(TxOp::InsertDocs { table, .. } | TxOp::PutDocs { table, .. } | TxOp::PutArrowDocs { table, .. } | TxOp::PatchDocs { table, .. } | TxOp::DeleteDocs { table, .. } | TxOp::EraseDocs { table, .. })
        | Operation::InsertIids { table, .. } | Operation::InsertArrowIids { table, .. } | Operation::PutIids { table, .. } | Operation::PutReplayArrow { table, .. } | Operation::PatchIids { table, .. } | Operation::PatchArrowIids { table, .. } => table,
        _ => return Ok(()),
    };
    parse_table(&table_name(table))?;
    if is_reserved_write_schema(&table.schema) {
        return Err(Anomaly::incorrect("xtdb/forbidden-table", format!("Cannot write to table: {table}"))
            .with_context("schema", table.schema.clone().into()).with_context("table", table.table.clone().into()));
    }
    Ok(())
}
fn parse_table(name: &str) -> Result<TableRef> {
    let (schema, table) = name.split_once('/').ok_or_else(|| invalid("Source table must use schema/table"))?;
    if schema.is_empty() || table.is_empty() || table.contains('/') { return Err(invalid("Invalid source table name")); }
    Ok(TableRef::new(schema, table))
}
fn overflow() -> Anomaly { invalid("Transaction allocation exceeds address space") }
fn invalid(message: impl Into<String>) -> Anomaly { Anomaly::incorrect("rtdb.tx/invalid-source-transaction", message) }

#[cfg(test)]
mod tests {
    use super::*;
    use rtdb_types::ListKind;

    fn budget() -> Arc<MemoryBudget> { MemoryBudget::new(128 * 1024 * 1024) }
    fn roundtrip(source: &SourceTransaction) -> SourceTransaction {
        let encoded = source.encode().unwrap();
        let message = SourceMessage::decode(encoded.as_slice()).unwrap().unwrap();
        SourceTransaction::decode(&message, &source.memory).unwrap()
    }

    #[test]
    fn ordered_world_clock_advance_keeps_its_parent_pin_without_point_checks() {
        let world = Iid([4; 16]);
        let pin = Iid([5; 16]);
        let operation = TxOp::OwnedPublishWorld {
            world, parent_pin: pin, attempt: "0000000000000000000000".into(),
            fingerprint: [1; 32], expected: Some(Timestamp(17)),
            advance_to: Some(Timestamp(22)), drop_world: false,
            settlement_pin: None, land_root: None, delete_root: None,
        };
        let source = SourceTransaction::new(
            vec![operation], SubmitOptions::default(), &budget(),
        ).unwrap();
        let replayed = roundtrip(&source);
        assert!(matches!(replayed.operations.as_slice(),
            [Operation::Public(TxOp::OwnedPublishWorld {
                world: actual, parent_pin, expected: Some(Timestamp(17)),
                advance_to: Some(Timestamp(22)), ..
            })] if *actual == world && *parent_pin == pin
        ));
    }

    #[test]
    fn canonical_arrow_sql_arguments_match_row_source_bytes() {
        use arrow_array::{BooleanArray, StringArray, UInt64Array};
        use arrow_buffer::{BooleanBuffer, Buffer, NullBuffer, ScalarBuffer};

        let memory = budget();
        let schema = Arc::new(Schema::new(vec![
            Field::new("unsigned", DataType::UInt64, true),
            Field::new("bool", DataType::Boolean, false),
            Field::new("label", DataType::Utf8, false).with_metadata([("client".into(), "binding".into())].into()),
        ]));
        let validity = NullBuffer::new(BooleanBuffer::new(Buffer::from_vec(vec![0xfb_u8]), 0, 3));
        let numbers = UInt64Array::new(ScalarBuffer::from(vec![8, u64::MAX, 999]), Some(validity));
        let flags = BooleanArray::new(BooleanBuffer::new(Buffer::from_vec(vec![0xff_u8]), 0, 3), None);
        let label = StringArray::from(vec!["prefix", "left", "right"]);
        let batch = RecordBatch::try_new(schema, vec![Arc::new(numbers), Arc::new(flags), Arc::new(label)]).unwrap().slice(1, 2);
        let bindings = SqlArguments::from_arrow_with_canonical_source(OwnedBatch::new(batch, &memory).unwrap(), &memory).unwrap();
        let rows = bindings.rows().map(|row| row.unwrap().to_vec()).collect();
        let reference = SqlArguments::new(bindings.types().to_vec(), rows, &memory).unwrap();
        assert_eq!(encode_arguments(&bindings, &memory).unwrap().as_slice(), encode_arguments(&reference, &memory).unwrap().as_slice());
    }

    #[test]
    fn arrow_sql_arguments_keep_their_buffers_and_replay_with_positional_fields() {
        let memory = budget();
        let schema = Arc::new(Schema::new(vec![Field::new("client_name", DataType::UInt64, true)]));
        let arguments = |values| {
            let column = Arc::new(arrow_array::UInt64Array::from(values));
            let batch = RecordBatch::try_new(Arc::clone(&schema), vec![column]).unwrap();
            let data = batch.column(0).to_data();
            let batch = OwnedBatch::new(batch, &memory).unwrap();
            let arguments = SqlArguments::from_arrow(batch, &memory).unwrap();
            let pages = arguments.uniform_arrow_pages().unwrap();
            assert_eq!(pages[0].column(0).to_data().buffers()[0].as_ptr(), data.buffers()[0].as_ptr());
            arguments
        };
        let mut arguments = arguments(vec![Some(u64::MAX), None]);
        let other = RecordBatch::try_new(schema, vec![Arc::new(arrow_array::UInt64Array::from(vec![5]))]).unwrap();
        arguments.append(SqlArguments::from_arrow(OwnedBatch::new(other, &memory).unwrap(), &memory).unwrap()).unwrap();
        let source = SourceTransaction::new(
            vec![TxOp::Sql { sql: "UPDATE docs SET v = ?".into(), args: Some(arguments) }],
            SubmitOptions::default(),
            &memory,
        ).unwrap();

        let replayed = roundtrip(&source);

        let [Operation::Public(TxOp::Sql { args: Some(arguments), .. })] = replayed.operations.as_slice() else {
            panic!("Expected SQL arguments");
        };
        let pages = arguments.uniform_arrow_pages().unwrap();
        assert_eq!(pages.len(), 1);
        assert_eq!(pages[0].schema_ref().field(0).name(), "?_0");
        assert_eq!(pages[0].schema_ref().field(0).data_type(), &DataType::UInt64);
        let rows: Vec<_> = arguments.indexed_rows().map(|row| {
            let (index, values) = row.unwrap();
            (index, values[0].clone())
        }).collect();
        assert_eq!(rows, [(0, Value::U64(u64::MAX)), (1, Value::Null), (2, Value::U64(5))]);
    }

    #[test]
    fn coalesced_argument_chunks_align_arrow_columns_and_replay_in_input_order() {
        let memory = budget();
        let page = |names: [&str; 2], row: [Value; 2]| {
            let types = row.each_ref().map(Value::logical_type);
            let fields = names.into_iter().zip(&types).map(|(name, ty)| to_field(name, ty).unwrap()).collect::<Vec<_>>();
            let columns = row.iter().zip(&types).map(|(value, ty)| build_array(ty, std::slice::from_ref(value)).unwrap()).collect();
            let batch = RecordBatch::try_new(Arc::new(Schema::new(fields)), columns).unwrap();
            SqlArguments::from_arrow(OwnedBatch::new(batch, &memory).unwrap(), &memory).unwrap()
        };
        let a = Value::List(vec![Value::I64(7)]);
        let b = Value::Bool(true);
        let record = |fields| Value::Struct(StructFields::from(fields));
        let mut arguments = page(["raw", "document"], [Value::U64(u64::MAX), record([("a".into(), a.clone())])]);
        arguments.append(page(["number", "details"], [Value::I64(9), record([("b".into(), b.clone())])])).unwrap();
        arguments.append(SqlArguments::from_rows(vec![vec![Value::Null, Value::Null]], &memory).unwrap()).unwrap();
        assert!(arguments.uniform_arrow_pages().is_none());

        let batch = arguments.source_columnar_batch(&memory).unwrap();
        let schema = batch.batch().schema();
        assert_eq!(schema.fields().iter().map(|field| field.name().as_str()).collect::<Vec<_>>(), ["?_0", "?_1"]);
        assert!(batch.batch().column(0).as_any().is::<UnionArray>());
        assert!(batch.batch().column(1).as_any().is::<StructArray>());
        let bytes = encode_arguments(&arguments, &memory).unwrap();
        let replayed = decode_arguments(bytes.as_slice(), &memory).unwrap();

        let rows = replayed.indexed_rows().map(|row| {
            let (ordinal, row) = row.unwrap();
            (ordinal, row.to_vec())
        }).collect::<Vec<_>>();
        assert_eq!(rows, [
            (0, vec![Value::U64(u64::MAX), Value::Struct(StructFields::from([("a".into(), a), ("b".into(), Value::Null)]))]),
            (1, vec![Value::I64(9), Value::Struct(StructFields::from([("a".into(), Value::Null), ("b".into(), b)]))]),
            (2, vec![Value::Null, Value::Null]),
        ]);
    }

    #[test]
    fn coalesced_argument_chunks_keep_explicit_union_nulls_inside_lists() {
        let memory = budget();
        let element = LogicalType::from_legs([LogicalType::Scalar(ScalarType::I64), LogicalType::Scalar(ScalarType::Utf8), LogicalType::Null]);
        let types = [ListKind::List, ListKind::FixedSizeList(2)].map(|kind| LogicalType::List { kind, element: Box::new(element.clone()) });
        let chunk = |names: [&str; 2], values: &[Value; 2]| {
            let fields = names.into_iter().zip(&types).map(|(name, ty)| to_field(name, ty).unwrap()).collect::<Vec<_>>();
            let arrays = values.iter().zip(&types).map(|(value, ty)| build_array(ty, std::slice::from_ref(value)).unwrap()).collect();
            let batch = RecordBatch::try_new(Arc::new(Schema::new(fields)), arrays).unwrap();
            SqlArguments::from_arrow(OwnedBatch::new(batch, &memory).unwrap(), &memory).unwrap()
        };
        let first = [Value::List(vec![Value::I64(4), Value::Null]), Value::FixedSizeList(vec![Value::Null, Value::Utf8("x".into())])];
        let second = [Value::List(vec![Value::Null]), Value::FixedSizeList(vec![Value::I64(5), Value::Null])];
        let mut arguments = chunk(["left_list", "left_pair"], &first);
        arguments.append(chunk(["right_list", "right_pair"], &second)).unwrap();
        assert!(arguments.uniform_arrow_pages().is_none());
        assert_eq!(arguments.source_columnar_batch(&memory).unwrap().batch().num_rows(), 2);

        let bytes = encode_arguments(&arguments, &memory).unwrap();
        let replayed = decode_arguments(bytes.as_slice(), &memory).unwrap();

        assert_eq!(replayed.rows().map(|row| row.unwrap().to_vec()).collect::<Vec<_>>(), [first.to_vec(), second.to_vec()]);
    }

    #[test]
    fn coalesced_argument_chunks_discard_children_of_null_structs_and_lists() {
        let memory = budget();
        let binary = Arc::new(Field::new("payload", DataType::Binary, false));
        let payload = vec![19; 65536];
        let values = Arc::new(arrow_array::BinaryArray::from_iter_values([payload.as_slice(), b"keep"]));
        let mask = arrow_buffer::NullBuffer::from(vec![false, true]);
        let records = rtdb_columnar::build_struct_array(vec![Arc::clone(&binary)].into(), &[values.clone()], 2, Some(mask.clone())).unwrap();
        let lists: ArrayRef = Arc::new(ListArray::try_new(binary, arrow_buffer::OffsetBuffer::new(vec![0i32, 1, 2].into()), values, Some(mask)).unwrap());
        let chunk = |names: [&str; 2], row| {
            let fields = [records.data_type(), lists.data_type()].into_iter().zip(names)
                .map(|(ty, name)| Field::new(name, ty.clone(), true)).collect::<Vec<_>>();
            let batch = RecordBatch::try_new(Arc::new(Schema::new(fields)), vec![records.slice(row, 1), lists.slice(row, 1)]).unwrap();
            SqlArguments::from_arrow(OwnedBatch::new(batch, &memory).unwrap(), &memory).unwrap()
        };
        let mut arguments = chunk(["record", "list"], 0);
        arguments.append(chunk(["document", "items"], 1)).unwrap();
        let batch = arguments.source_columnar_batch(&memory).unwrap();
        let records = batch.batch().column(0).as_struct();
        let lists = batch.batch().column(1).as_list::<i32>();
        assert_eq!(records.column(0).as_binary::<i32>().value_data(), b"keep");
        assert_eq!(lists.values().as_binary::<i32>().value_data(), b"keep");
        assert_eq!(lists.value_offsets(), &[0, 0, 1]);

        let bytes = encode_arguments(&arguments, &memory).unwrap();
        let replayed = decode_arguments(bytes.as_slice(), &memory).unwrap();

        assert!(bytes.as_slice().len() < payload.len());
        assert_eq!(&*replayed.rows().next().unwrap().unwrap(), &[Value::Null, Value::Null]);
    }

    #[test]
    fn coalescing_rejects_a_null_value_in_a_nonnullable_arrow_union() {
        let memory = budget();
        let ty = LogicalType::Scalar(ScalarType::I64).join(&LogicalType::Scalar(ScalarType::Utf8));
        let chunk = |name, value: Option<i64>| {
            let field = to_field(name, &ty).unwrap();
            let DataType::Union(legs, UnionMode::Dense) = field.data_type() else { panic!("Expected union"); };
            let array = UnionArray::try_new(legs.clone(), vec![0i8].into(), Some(vec![0i32].into()), vec![
                Arc::new(arrow_array::Int64Array::from(vec![value])),
                Arc::new(arrow_array::StringArray::from(Vec::<&str>::new())),
            ]).unwrap();
            let batch = RecordBatch::try_new(Arc::new(Schema::new(vec![field])), vec![Arc::new(array)]).unwrap();
            SqlArguments::from_arrow(OwnedBatch::new(batch, &memory).unwrap(), &memory).unwrap()
        };
        let mut arguments = chunk("a", None);
        arguments.append(chunk("b", Some(7))).unwrap();
        assert!(arguments.uniform_arrow_pages().is_none());

        let error = encode_arguments(&arguments, &memory).unwrap_err();

        assert_eq!(error.code, "rtdb/column-type-mismatch");
    }

    #[test]
    fn an_arrow_put_matches_the_source_codec_for_the_same_documents() {
        let memory = budget();
        let schema = Arc::new(Schema::new(vec![Field::new("_id", DataType::Int64, true), Field::new("v", DataType::Utf8, true)]));
        let batch = RecordBatch::try_new(schema, vec![
            Arc::new(arrow_array::Int64Array::from(vec![1, 2])),
            Arc::new(arrow_array::StringArray::from(vec![Some("text"), None])),
        ]).unwrap();
        let table = TableRef::new("public", "docs");
        let rows = vec![
            StructFields::from([("_id".into(), Value::I64(1)), ("v".into(), Value::Utf8("text".into()))]),
            StructFields::from([("_id".into(), Value::I64(2)), ("v".into(), Value::Null)]),
        ];
        let row_put = SourceTransaction::new(vec![TxOp::PutDocs {
            table: table.clone(), documents: Documents::new(rows, &memory).unwrap(),
            valid_from: None, valid_to: None,
        }], SubmitOptions::default(), &memory).unwrap();
        let batch = OwnedBatch::new(batch, &memory).unwrap();
        let documents = ArrowDocuments::from_batch(&batch, &memory).unwrap().unwrap();
        let arrow_put = SourceTransaction::new(vec![TxOp::PutArrowDocs { table, documents }], SubmitOptions::default(), &memory).unwrap();

        assert_eq!(row_put.encode().unwrap().as_slice(), arrow_put.encode().unwrap().as_slice());
        let replayed = roundtrip(&arrow_put);
        let [Operation::PutReplayArrow { documents, iids: Some(iids), .. }] = replayed.operations.as_slice() else { panic!("expected columnar replay"); };
        assert_eq!(documents.array().len(), 2);
        assert_eq!(iids, &[EntityId::Integer(1).iid(), EntityId::Integer(2).iid()]);
    }

    #[test]
    fn uuid_uploads_keep_their_arrow_types_and_nulls_on_source_replay() {
        let memory = budget();
        let table = TableRef::new("public", "docs");
        let uuid = LogicalType::Scalar(rtdb_types::ScalarType::Uuid);
        let insert = |id, logical: &LogicalType, value| {
            let schema = Arc::new(Schema::new(vec![
                Field::new("_id", DataType::Int64, false), to_field("v", logical).unwrap(),
            ]));
            let batch = RecordBatch::try_new(schema, vec![
                Arc::new(arrow_array::Int64Array::from(vec![id])), rtdb_columnar::copy_array(logical, &[value]).unwrap(),
            ]).unwrap();
            let batch = OwnedBatch::new(batch, &memory).unwrap();
            let documents = ArrowDocuments::from_batch(&batch, &memory).unwrap().unwrap();
            TxOp::PutArrowDocs { table: table.clone(), documents }
        };
        let opaque = Value::FixedSizeBinary(vec![3; 16]);
        let submissions = vec![insert(1, &uuid, Value::Uuid([3; 16])), insert(2, &opaque.logical_type(), opaque.clone()),
            insert(3, &uuid.maybe(), Value::Null)];
        let transaction = SourceTransaction::new(submissions, SubmitOptions::default(), &memory).unwrap();

        let replay = roundtrip(&transaction);

        for (operation, expected) in replay.operations.iter().zip([Value::Uuid([3; 16]), opaque, Value::Null]) {
            let Operation::PutReplayArrow { documents, .. } = operation else { panic!("Expected an Arrow operation") };
            assert_eq!(documents.to_rows(false, &memory).unwrap().rows()[0]["v"], expected);
        }
        assert_eq!(replay.operations.len(), 3);
    }

    #[test]
    fn an_all_null_uuid_id_reaches_the_missing_id_error() {
        let memory = budget();
        let uuid = LogicalType::Scalar(rtdb_types::ScalarType::Uuid).maybe();
        let schema = Arc::new(Schema::new(vec![to_field("_id", &uuid).unwrap()]));
        let batch = RecordBatch::try_new(schema, vec![rtdb_columnar::copy_array(&uuid, &[Value::Null]).unwrap()]).unwrap();
        let batch = OwnedBatch::new(batch, &memory).unwrap();
        let documents = ArrowDocuments::from_batch(&batch, &memory).unwrap().unwrap();
        let table = TableRef::new("public", "docs");
        let transaction = SourceTransaction::new(vec![TxOp::PutArrowDocs { table, documents }], SubmitOptions::default(), &memory).unwrap();

        let error = transaction.encode().unwrap_err();

        assert_eq!(error.code, "missing-id");
        assert!(error.context.contains_key("doc"));
    }

    #[test]
    fn a_tagged_null_in_a_uuid_id_column_reports_a_missing_id() {
        let memory = budget();
        let values = [Value::Uuid([3; 16]), Value::Null, Value::I64(1)];
        let logical = LogicalType::join_all(&values.iter().map(Value::logical_type).collect::<Vec<_>>());
        let fields = vec![to_field("_id", &logical).unwrap()].into();
        let array = rtdb_columnar::copy_array(&logical, &values).unwrap();
        let layout = DecodedDocumentLayout::new(&fields, &memory).unwrap();
        let array = rtdb_columnar::build_struct_array(fields, &[array], values.len(), None).unwrap();
        let documents = ArrowDocuments::from_source(array.as_struct(), layout, array.get_array_memory_size(), &memory).unwrap().unwrap();
        let reader = documents.id_reader().unwrap();

        let error = documents.iid(1, reader.as_ref(), &memory).unwrap_err();

        assert_eq!(error.code, "missing-id");
        assert!(error.context.contains_key("doc"));
    }

    #[test]
    fn uuid_uploads_keep_row_codec_bytes_for_nulls_with_hidden_payloads() {
        use arrow_array::{FixedSizeBinaryArray, Int64Array};
        use arrow_buffer::{Buffer, NullBuffer};

        let memory = budget();
        let logical = LogicalType::Scalar(rtdb_types::ScalarType::Uuid).maybe();
        let schema = Arc::new(Schema::new(vec![Field::new("_id", DataType::Int64, false), to_field("v", &logical).unwrap()]));
        let upload = |validity| {
            let bytes = Buffer::from_vec(vec![3_u8; 32]);
            let column = FixedSizeBinaryArray::new(16, bytes, Some(NullBuffer::from(validity)));
            let batch = RecordBatch::try_new(Arc::clone(&schema), vec![Arc::new(Int64Array::from(vec![1, 2])), Arc::new(column)]).unwrap();
            let batch = OwnedBatch::new(batch, &memory).unwrap();
            ArrowDocuments::from_batch(&batch, &memory).unwrap()
        };
        let table = TableRef::new("public", "docs");
        let documents = upload(vec![true, false]).unwrap();
        let puts = SourceTransaction::new(vec![TxOp::PutArrowDocs { table: table.clone(), documents }], SubmitOptions::default(), &memory).unwrap();
        let rows = vec![StructFields::from([("_id".into(), Value::I64(1)), ("v".into(), Value::Uuid([3; 16]))]),
            StructFields::from([("_id".into(), Value::I64(2)), ("v".into(), Value::Null)])];
        let row_puts = SourceTransaction::new(vec![TxOp::PutDocs { table, documents: Documents::new(rows, &memory).unwrap(),
            valid_from: None, valid_to: None }], SubmitOptions::default(), &memory).unwrap();

        assert_eq!(puts.encode().unwrap().as_slice(), row_puts.encode().unwrap().as_slice());
        assert!(upload(vec![false, true]).is_none());
    }

    #[test]
    fn arrow_document_batches_combine_missing_columns_in_first_seen_order() {
        let memory = budget();
        let first = StructArray::try_new(
            vec![Field::new("a", DataType::Utf8, false), Field::new("_id", DataType::Int64, false)].into(),
            vec![Arc::new(arrow_array::StringArray::from(vec!["left"])), Arc::new(arrow_array::Int64Array::from(vec![1]))],
            None,
        ).unwrap();
        let second = StructArray::try_new(
            vec![Field::new("_id", DataType::Int64, false), Field::new("b", DataType::Boolean, false)].into(),
            vec![Arc::new(arrow_array::Int64Array::from(vec![2])), Arc::new(arrow_array::BooleanArray::from(vec![true]))],
            None,
        ).unwrap();
        let mut arrays = BatchBuilder::new(4096, &memory).unwrap();

        let result = arrow_document_leg(&[&first, &second], &mut arrays).unwrap().unwrap();

        let lists = result.as_any().downcast_ref::<ListArray>().unwrap();
        assert_eq!(lists.value_offsets(), &[0, 1, 2]);
        let values = lists.values().as_any().downcast_ref::<StructArray>().unwrap();
        assert_eq!(values.fields().iter().map(|field| field.name().as_str()).collect::<Vec<_>>(), ["a", "_id", "b"]);
        assert_eq!(read_value(&values.fields()[0], values.column(0).as_ref(), 0).unwrap(), Value::Utf8("left".into()));
        assert!(values.column(0).is_null(1));
        assert!(values.column(2).is_null(0));
        assert_eq!(read_value(&values.fields()[2], values.column(2).as_ref(), 1).unwrap(), Value::Bool(true));
    }

    #[test]
    fn arrow_puts_with_different_scalar_types_replay_as_dense_unions() {
        let memory = budget();
        let put = |id, value: ArrayRef| {
            let schema = Arc::new(Schema::new(vec![
                Field::new("_id", DataType::Int64, false),
                Field::new("v", value.data_type().clone(), false),
            ]));
            let batch = RecordBatch::try_new(schema, vec![Arc::new(arrow_array::Int64Array::from(vec![id])), value]).unwrap();
            let batch = OwnedBatch::new(batch, &memory).unwrap();
            let documents = ArrowDocuments::from_batch(&batch, &memory).unwrap().unwrap();
            TxOp::PutArrowDocs { table: TableRef::new("public", "docs"), documents }
        };
        let source = SourceTransaction::new(vec![
            put(1, Arc::new(arrow_array::Int64Array::from(vec![7]))),
            put(2, Arc::new(arrow_array::StringArray::from(vec!["text"]))),
        ], SubmitOptions::default(), &memory).unwrap();

        let replay = roundtrip(&source);

        let expected = [Value::I64(7), Value::Utf8("text".into())];
        for (operation, expected) in replay.operations.iter().zip(expected) {
            let Operation::PutReplayArrow { documents, .. } = operation else {
                panic!("Expected Arrow documents on replay");
            };
            let array = documents.array().column_by_name("v").unwrap();
            let field = documents.array().fields().iter().find(|field| field.name() == "v").unwrap();
            assert!(array.as_any().is::<UnionArray>());
            assert_eq!(read_value(field, array.as_ref(), 0).unwrap(), expected);
        }
    }

    #[test]
    fn mixed_arrow_scalar_layouts_keep_null_and_missing_values_in_the_union() {
        let memory = budget();
        let table = TableRef::new("public", "docs");
        let put = |ids: Vec<i64>, column: Option<ArrayRef>| {
            let mut fields = vec![Field::new("_id", DataType::Int64, false)];
            let mut columns = vec![Arc::new(arrow_array::Int64Array::from(ids)) as ArrayRef];
            if let Some(column) = column {
                fields.push(Field::new("v", column.data_type().clone(), true));
                columns.push(column);
            }
            let batch = RecordBatch::try_new(Arc::new(Schema::new(fields)), columns).unwrap();
            let batch = OwnedBatch::new(batch, &memory).unwrap();
            let documents = ArrowDocuments::from_batch(&batch, &memory).unwrap().unwrap();
            TxOp::PutArrowDocs { table: table.clone(), documents }
        };
        let source = SourceTransaction::new(vec![
            put(vec![1, 2, 3], Some(Arc::new(arrow_array::Int64Array::from(vec![Some(7), None, Some(9)])))),
            put(vec![4], Some(Arc::new(arrow_array::StringArray::from(vec!["text"])))),
            put(vec![5], None),
            put(vec![6], Some(Arc::new(arrow_array::NullArray::new(1)))),
        ], SubmitOptions::default(), &memory).unwrap();

        let replay = roundtrip(&source);

        let expected = [vec![Value::I64(7), Value::Null, Value::I64(9)], vec![Value::Utf8("text".into())], vec![Value::Null], vec![Value::Null]];
        assert_eq!(replay.operations.len(), expected.len());
        for (operation, expected) in replay.operations.iter().zip(expected) {
            let Operation::PutReplayArrow { documents, .. } = operation else {
                panic!("Expected Arrow documents on replay");
            };
            let array = documents.array().column_by_name("v").unwrap();
            let field = documents.array().fields().iter().find(|field| field.name() == "v").unwrap();
            assert!(array.as_any().is::<UnionArray>());
            let values = (0..array.len()).map(|row| read_value(field, array.as_ref(), row).unwrap()).collect::<Vec<_>>();
            assert_eq!(values, expected);
        }
    }

    #[test]
    fn different_nested_container_kinds_replay_with_typed_union_children() {
        let memory = budget();
        let record = |fields: &[(&str, Value)]| Value::Struct(fields.iter()
            .map(|(name, value)| (name.to_string(), value.clone())).collect());
        let put = |id, values: Vec<Value>| {
            let logical = LogicalType::join_all(values.iter().map(Value::logical_type).collect::<Vec<_>>().iter());
            let fields = vec![Field::new("_id", DataType::Int64, false), to_field("v", &logical).unwrap()];
            let columns = vec![
                Arc::new(arrow_array::Int64Array::from_iter_values(id..id + values.len() as i64)) as ArrayRef,
                build_array(&logical, &values).unwrap(),
            ];
            let batch = RecordBatch::try_new(Arc::new(Schema::new(fields)), columns).unwrap();
            let batch = OwnedBatch::new(batch, &memory).unwrap();
            let documents = ArrowDocuments::from_batch(&batch, &memory).unwrap().unwrap();
            TxOp::PutArrowDocs { table: TableRef::new("public", "docs"), documents }
        };
        let list = Value::List(vec![Value::Utf8("list".into())]);
        let source = SourceTransaction::new(vec![
            put(1, vec![Value::Null, record(&[("a", Value::I64(7))]), record(&[("a", Value::I64(9))])]),
            put(4, vec![list.clone(), Value::Null]),
            put(6, vec![record(&[("b", Value::Bool(true))])]),
        ], SubmitOptions::default(), &memory).unwrap();
        let replay = roundtrip(&source);

        let expected = [
            vec![Value::Null, record(&[("a", Value::I64(7)), ("b", Value::Null)]), record(&[("a", Value::I64(9)), ("b", Value::Null)])],
            vec![list, Value::Null],
            vec![record(&[("a", Value::Null), ("b", Value::Bool(true))])],
        ];
        assert_eq!(replay.operations.len(), expected.len());
        for (operation, expected) in replay.operations.iter().zip(expected) {
            let Operation::PutReplayArrow { documents, .. } = operation else { panic!("Expected Arrow documents on replay"); };
            let v = documents.array().column_by_name("v").unwrap();
            let field = documents.array().fields().iter().find(|field| field.name() == "v").unwrap();
            let union = v.as_any().downcast_ref::<UnionArray>().unwrap();
            let DataType::Union(fields, UnionMode::Dense) = field.data_type() else { panic!("Expected a dense union"); };
            assert_eq!(fields.iter().map(|(_, field)| field.name().as_str()).collect::<Vec<_>>(), ["struct", "null", "list"]);
            for (row, value) in expected.into_iter().enumerate() {
                assert_eq!(read_value(field, union, row).unwrap(), value);
            }
        }
    }

    #[test]
    fn mixed_documents_encode_under_a_budget_that_cannot_materialize_arrow_rows() {
        let memory = MemoryBudget::new(512 * 1024);
        let schema = Arc::new(Schema::new(vec![Field::new("flag", DataType::Boolean, false)]));
        let flag = Arc::new(arrow_array::BooleanArray::from(vec![true; 8192]));
        let batch = OwnedBatch::new(RecordBatch::try_new(schema, vec![flag]).unwrap(), &memory).unwrap();
        let arrow = ArrowDocuments::from_batch(&batch, &memory).unwrap().unwrap();
        let rows = Documents::new(vec![StructFields::from([("flag".into(), Value::Bool(false))])], &memory).unwrap();
        assert!(arrow.to_rows(false, &memory).is_err());
        let mut arrays = BatchBuilder::new(4096, &memory).unwrap();

        let documents = document_leg(vec![SourceDocuments::Arrow(&arrow), SourceDocuments::Rows(&rows)], &mut arrays, &memory).unwrap();

        let documents = documents.as_any().downcast_ref::<ListArray>().unwrap();
        let values = documents.values().as_any().downcast_ref::<StructArray>().unwrap();
        let flags = values.column(0).as_any().downcast_ref::<arrow_array::BooleanArray>().unwrap();
        assert_eq!(documents.value_offsets(), [0, 8192, 8193]);
        assert!(flags.iter().take(8192).all(|value| value == Some(true)));
        assert_eq!(flags.iter().last(), Some(Some(false)));
    }

    #[test]
    fn row_and_arrow_puts_share_the_source_layout_in_transaction_order() {
        let memory = budget();
        let table = TableRef::new("public", "docs");
        let values = [vec![Value::I64(7), Value::Null], vec![Value::List(vec![Value::Bool(true)])]];
        let rows = {
            let mut first = StructFields::from([("row_field".into(), Value::Bool(true)), ("_id".into(), Value::I64(1))]);
            first.insert("v".into(), values[0][0].clone());
            let second = StructFields::from([("_id".into(), Value::I64(2))]);
            let documents = Documents::new(vec![first, second], &memory).unwrap();
            TxOp::PutDocs { table: table.clone(), documents, valid_from: None, valid_to: None }
        };
        let arrow = {
            let logical = values[1][0].logical_type();
            let fields = vec![Field::new("_id", DataType::Int64, false), to_field("v", &logical).unwrap(), Field::new("arrow_field", DataType::Utf8, false)];
            let columns = vec![Arc::new(arrow_array::Int64Array::from(vec![3])) as ArrayRef,
                build_array(&logical, &values[1]).unwrap(), Arc::new(arrow_array::StringArray::from(vec!["arrow"]))];
            let batch = OwnedBatch::new(RecordBatch::try_new(Arc::new(Schema::new(fields)), columns).unwrap(), &memory).unwrap();
            let documents = ArrowDocuments::from_batch(&batch, &memory).unwrap().unwrap();
            TxOp::PutArrowDocs { table: table.clone(), documents }
        };
        let source = SourceTransaction::new(vec![rows, arrow], SubmitOptions::default(), &memory).unwrap();

        let replay = roundtrip(&source);

        assert_eq!(replay.operations.len(), values.len());
        let ids: [&[i64]; 2] = [&[1, 2], &[3]];
        for (position, ((operation, values), ids)) in replay.operations.iter().zip(values).zip(ids).enumerate() {
            let Operation::PutReplayArrow { documents, iids: Some(iids), .. } = operation else { panic!("Expected Arrow documents on replay"); };
            let batch = documents.array();
            assert_eq!(documents.columns(), ["row_field", "_id", "v", "arrow_field"]);
            assert_eq!(iids, &ids.iter().map(|id| EntityId::Integer(*id).iid()).collect::<Vec<_>>());
            assert_eq!(batch.column(0).is_valid(0), position != 1);
            assert_eq!(batch.column(3).is_valid(0), position == 1);
            let union = batch.column(2).as_union();
            let DataType::Union(legs, UnionMode::Dense) = union.data_type() else { panic!("Expected a dense union"); };
            assert_eq!(legs.iter().map(|(_, field)| field.name().as_str()).collect::<Vec<_>>(), ["i64", "list", "null"]);
            for (row, value) in values.into_iter().enumerate() {
                assert_eq!(read_value(&batch.fields()[2], union, row).unwrap(), value);
            }
        }
    }

    #[test]
    fn mixed_documents_normalize_arrow_union_aliases_before_replay() {
        let memory = budget();
        let table = TableRef::new("public", "docs");
        let ints = || Arc::new(arrow_array::Int64Array::from(vec![7])) as ArrayRef;
        let int = Field::new("i64", DataType::Int64, false);
        let cases = [
            (vec![3, 8], vec![int.clone(), Field::new("null", DataType::Null, true)],
                vec![ints(), Arc::new(arrow_array::NullArray::new(1)) as ArrayRef], vec![3, 8], vec![Value::I64(7), Value::Null]),
            (vec![3, 8, 9], vec![int.clone(), int.with_name("second_i64"), Field::new("utf8", DataType::Utf8, false)],
                vec![ints(), ints(), Arc::new(arrow_array::StringArray::from(vec!["s"]))], vec![8, 9], vec![Value::I64(7), Value::Utf8("s".into())]),
            (vec![3, 8, 9], vec![Field::new("a", DataType::UInt64, false), Field::new("b", DataType::Int64, false), Field::new("s", DataType::Utf8, false)],
                vec![Arc::new(arrow_array::UInt64Array::from(vec![u64::MAX])), ints(), Arc::new(arrow_array::StringArray::from(vec!["s"]))],
                vec![3, 9], vec![Value::I64(-1), Value::Utf8("s".into())]),
        ];
        for (tags, fields, children, selected, expected) in cases {
            let legs = UnionFields::try_new(tags, fields).unwrap();
            let column = UnionArray::try_new(legs, selected.into(), Some(vec![0i32, 0].into()), children).unwrap();
            let schema = Arc::new(Schema::new(vec![Field::new("_id", DataType::Int64, false), Field::new("v", column.data_type().clone(), false)]));
            let batch = RecordBatch::try_new(schema, vec![Arc::new(arrow_array::Int64Array::from(vec![1, 2])), Arc::new(column)]).unwrap();
            let batch = OwnedBatch::new(batch, &memory).unwrap();
            let arrow = ArrowDocuments::from_batch(&batch, &memory).unwrap().unwrap();
            drop(batch);
            let (_, column) = document_column(arrow.array(), "v").unwrap();
            match column.data_type() {
                DataType::Int64 => assert_eq!(expected[1], Value::Null),
                DataType::Union(legs, UnionMode::Dense) => {
                    assert_ne!(expected[1], Value::Null);
                    assert_eq!(legs.iter().map(|(tag, field)| (tag, field.name().as_str())).collect::<Vec<_>>(), [(0, "i64"), (1, "utf8")]);
                }
                other => panic!("Unexpected normalized layout {other:?}"),
            }
            let row = StructFields::from([("_id".into(), Value::I64(3)), ("v".into(), Value::I64(9))]);
            let rows = Documents::new(vec![row], &memory).unwrap();
            let source = SourceTransaction::new(vec![
                TxOp::PutArrowDocs { table: table.clone(), documents: arrow },
                TxOp::PutDocs { table: table.clone(), documents: rows, valid_from: None, valid_to: None },
            ], SubmitOptions::default(), &memory).unwrap();

            let replay = roundtrip(&source);

            assert_eq!(replay.operations.len(), 2);
            for (operation, expected) in replay.operations.iter().zip([expected, vec![Value::I64(9)]]) {
                let Operation::PutReplayArrow { documents, .. } = operation else { panic!("Expected Arrow documents on replay"); };
                let array = documents.array();
                let (field, values) = document_column(array, "v").unwrap();
                let actual = (0..array.len()).map(|row| read_value(field, values.as_ref(), row).unwrap()).collect::<Vec<_>>();
                assert_eq!(actual, expected);
            }
        }
    }

    #[test]
    fn nullable_one_kind_union_inside_a_list_keeps_its_declared_nullability() {
        for (tags, expected) in [
            (vec![3i8, 8, 3], [Some(7), None, Some(7)]),
            (vec![3i8, 3, 3], [Some(7), Some(7), Some(7)]),
        ] {
            let memory = budget();
            let nullable = tags.contains(&8);
            let legs = UnionFields::try_new([3, 8], [Field::new("i64", DataType::Int64, false), Field::new("null", DataType::Null, true)]).unwrap();
            let items = UnionArray::try_new(legs, tags.into(), Some(vec![0i32, 0, 0].into()),
                vec![Arc::new(arrow_array::Int64Array::from(vec![7])), Arc::new(arrow_array::NullArray::new(usize::from(nullable)))]).unwrap();
            let item = Arc::new(Field::new("$data$", items.data_type().clone(), nullable));
            let list = ListArray::try_new(item, OffsetBuffer::new(vec![0i32, 3].into()), Arc::new(items), None).unwrap();
            let schema = Arc::new(Schema::new(vec![Field::new("v", list.data_type().clone(), false)]));
            let batch = OwnedBatch::new(RecordBatch::try_new(schema, vec![Arc::new(list)]).unwrap(), &memory).unwrap();
            let documents = ArrowDocuments::from_batch(&batch, &memory).unwrap().unwrap();
            drop(batch);

            let (field, list) = document_column(documents.array(), "v").unwrap();
            let list = list.as_any().downcast_ref::<ListArray>().unwrap();
            let DataType::List(item) = field.data_type() else { panic!("Expected normalized Arrow list"); };
            assert!(item.is_nullable());
            assert_eq!(list.values().as_primitive::<arrow_array::types::Int64Type>().iter().collect::<Vec<_>>(), expected);
            drop(documents);
            assert_eq!(memory.used_bytes(), 0);
        }
    }

    #[test]
    fn duplicate_union_tags_keep_input_owned_when_normalization_cannot_reserve() {
        let memory = MemoryBudget::new(384 * 1024);
        let legs = UnionFields::try_new([3, 8], [Field::new("left", DataType::Binary, false), Field::new("right", DataType::Binary, false)]).unwrap();
        let left = vec![1; 64 * 1024];
        let right = vec![2; 64 * 1024];
        let array = UnionArray::try_new(legs, vec![3i8, 8].into(), Some(vec![0i32, 0].into()), vec![
            Arc::new(arrow_array::BinaryArray::from(vec![left.as_slice()])),
            Arc::new(arrow_array::BinaryArray::from(vec![right.as_slice()])),
        ]).unwrap();
        let schema = Arc::new(Schema::new(vec![Field::new("v", array.data_type().clone(), false)]));
        let batch = OwnedBatch::new(RecordBatch::try_new(schema, vec![Arc::new(array)]).unwrap(), &memory).unwrap();

        let documents = ArrowDocuments::from_batch(&batch, &memory).unwrap().unwrap();

        assert!(Arc::ptr_eq(documents.array().column(0), batch.batch().column(0)));
    }

    #[test]
    fn optional_union_copy_does_not_displace_document_schema_admission() {
        let mut fields = (0..96).map(|index| Field::new(format!("v{index}"), DataType::Int64, false)).collect::<Vec<_>>();
        let mut columns = vec![Arc::new(arrow_array::Int64Array::from(vec![1, 2])) as ArrayRef; fields.len()];
        let legs = UnionFields::try_new([3, 8], [Field::new("a", DataType::Binary, false), Field::new("b", DataType::Binary, false)]).unwrap();
        let payload = vec![0u8; 8 * 1024];
        let child = || Arc::new(arrow_array::BinaryArray::from(vec![payload.as_slice()])) as ArrayRef;
        let union = UnionArray::try_new(legs, vec![3i8, 8].into(), Some(vec![0i32, 0].into()), vec![child(), child()]).unwrap();
        fields.push(Field::new("payload", union.data_type().clone(), false));
        columns.push(Arc::new(union));
        let schema = Arc::new(Schema::new(fields));
        let bound = rtdb_format::ipc::schema_translation_bound(&schema).unwrap();
        let batch = RecordBatch::try_new(schema, columns).unwrap();
        let memory = MemoryBudget::new(batch.get_array_memory_size() + 2 * bound);
        let batch = OwnedBatch::new(batch, &memory).unwrap();

        let documents = ArrowDocuments::from_batch(&batch, &memory).unwrap().unwrap();

        assert!(Arc::ptr_eq(documents.array().column(96), batch.batch().column(96)));
    }

    #[test]
    fn an_arrow_document_keeps_distinct_shapes_in_same_kind_union_children() {
        let memory = budget();
        let a = Value::Struct(StructFields::from([("a".into(), Value::I64(7))]));
        let b = Value::Struct(StructFields::from([("b".into(), Value::I64(9))]));
        let children = [&a, &b].map(|value| build_array(&value.logical_type(), std::slice::from_ref(value)).unwrap());
        let legs = UnionFields::try_new([3, 8], children.iter().zip(["a", "b"]).map(|(child, name)| Field::new(name, child.data_type().clone(), false))).unwrap();
        let array = UnionArray::try_new(legs, vec![3i8, 8].into(), Some(vec![0i32, 0].into()), Vec::from(children)).unwrap();
        let schema = Arc::new(Schema::new(vec![Field::new("v", array.data_type().clone(), false)]));
        let batch = OwnedBatch::new(RecordBatch::try_new(schema, vec![Arc::new(array)]).unwrap(), &memory).unwrap();

        let documents = ArrowDocuments::from_batch(&batch, &memory).unwrap().unwrap();

        let (field, array) = document_column(documents.array(), "v").unwrap();
        assert!(Arc::ptr_eq(array, batch.batch().column(0)));
        assert_eq!(read_value(field, array.as_ref(), 0).unwrap(), a);
        assert_eq!(read_value(field, array.as_ref(), 1).unwrap(), b);
    }

    #[test]
    fn mixed_fixed_size_lists_replay_from_arrow_columns() {
        let memory = budget();
        let first = Value::FixedSizeList(vec![Value::U64(u64::MAX), Value::U64(7)]);
        let second = Value::FixedSizeList(vec![Value::Utf8("a".into()), Value::Utf8("b".into())]);
        let put = |id, value: &Value| {
            let ty = value.logical_type();
            let schema = Arc::new(Schema::new(vec![Field::new("_id", DataType::Int64, false), to_field("items", &ty).unwrap()]));
            let columns = vec![
                Arc::new(arrow_array::Int64Array::from(vec![id])) as ArrayRef,
                build_array(&ty, std::slice::from_ref(value)).unwrap(),
            ];
            let batch = RecordBatch::try_new(schema, columns).unwrap();
            let batch = OwnedBatch::new(batch, &memory).unwrap();
            ArrowDocuments::from_batch(&batch, &memory).unwrap().unwrap()
        };
        let first_docs = put(1, &first);
        let second_docs = put(2, &second);
        let mut builder = BatchBuilder::new(4096, &memory).unwrap();
        assert!(arrow_document_leg(&[first_docs.array(), second_docs.array()], &mut builder).unwrap().is_some());
        let operations = [first_docs, second_docs].into_iter().map(|documents| TxOp::PutArrowDocs {
            table: TableRef::new("public", "docs"), documents,
        }).collect();
        let source = SourceTransaction::new(operations, SubmitOptions::default(), &memory).unwrap();

        let replay = roundtrip(&source);

        assert_eq!(replay.operations.len(), 2);
        for (operation, expected) in replay.operations.iter().zip([Value::FixedSizeList(vec![Value::I64(-1), Value::I64(7)]), second]) {
            let Operation::PutReplayArrow { documents, .. } = operation else { panic!("Expected Arrow replay"); };
            let (field, array) = document_column(documents.array(), "items").unwrap();
            assert_eq!(read_value(field, array.as_ref(), 0).unwrap(), expected);
        }
    }

    #[test]
    fn joining_fixed_size_list_uploads_does_not_allocate_inactive_union_children() {
        let width = 1 << 20;
        let element = LogicalType::Scalar(ScalarType::I64).join(&LogicalType::Scalar(ScalarType::FixedSizeBinary(width)));
        let list = LogicalType::List { kind: rtdb_types::ListKind::FixedSizeList(1), element: Box::new(element) };
        let field = Arc::new(to_field("items", &list).unwrap());
        let page = |value: i64| {
            let column = build_array(&list, &[Value::FixedSizeList(vec![Value::I64(value)])]).unwrap();
            StructArray::try_new(vec![Arc::clone(&field)].into(), vec![column], None).unwrap()
        };
        let (first, second) = (page(5), page(7));
        let memory = MemoryBudget::new(1 << 18);
        let mut builder = BatchBuilder::new(4096, &memory).unwrap();

        let result = arrow_document_leg(&[&first, &second], &mut builder).unwrap().unwrap();

        let docs = result.as_list::<i32>().values().as_struct();
        let (field, array) = document_column(docs, "items").unwrap();
        assert_eq!(read_value(field, array.as_ref(), 0).unwrap(), Value::FixedSizeList(vec![Value::I64(5)]));
        assert_eq!(read_value(field, array.as_ref(), 1).unwrap(), Value::FixedSizeList(vec![Value::I64(7)]));
        assert!(result.get_array_memory_size() < width as usize);
    }

    #[test]
    fn joining_fixed_size_lists_backfills_wide_fields_only_in_visible_entries() {
        use arrow_buffer::NullBuffer;
        let width = 1 << 16;
        let doc = |value| Value::Struct(StructFields::from([("n".into(), Value::I64(value))]));
        let entries = (0..1001).map(doc).collect::<Vec<_>>();
        let child = entries[0].logical_type();
        let child_field = Arc::new(to_field("$data$", &child).unwrap());
        let entries = build_array(&child, &entries).unwrap();
        let left = Arc::new(ListArray::try_new(child_field, OffsetBuffer::new(vec![0_i32, 1, 1001].into()), entries, None).unwrap());
        let item = Arc::new(Field::new("$data$", left.data_type().clone(), false));
        let left = arrow_array::FixedSizeListArray::try_new(item, 1, left, Some(NullBuffer::from(vec![true, false]))).unwrap();
        let right = Value::FixedSizeList(vec![Value::List(vec![Value::Struct(StructFields::from([
            ("n".into(), Value::I64(44)), ("payload".into(), Value::FixedSizeBinary(vec![8; width])),
        ]))])]);
        let right = build_array(&right.logical_type(), &[right]).unwrap();
        let page = |input: ArrayRef, nullable| {
            let field = Field::new("items", input.data_type().clone(), nullable);
            StructArray::try_new(vec![field].into(), vec![input], None).unwrap()
        };
        let (first, second) = (page(Arc::new(left), true), page(right, false));
        let memory = MemoryBudget::new(2 << 20);
        let mut builder = BatchBuilder::new(4096, &memory).unwrap();

        let list = arrow_document_leg(&[&first, &second], &mut builder).unwrap().unwrap();

        let docs = list.as_list::<i32>().values().as_struct();
        let (field, array) = document_column(docs, "items").unwrap();
        let expected = Value::FixedSizeList(vec![Value::List(vec![Value::Struct(StructFields::from([
            ("n".into(), Value::I64(0)), ("payload".into(), Value::Null),
        ]))])]);
        assert_eq!(read_value(field, array.as_ref(), 0).unwrap(), expected);
        assert_eq!(read_value(field, array.as_ref(), 1).unwrap(), Value::Null);
        assert_eq!(array.as_fixed_size_list().values().as_list::<i32>().value_offsets(), &[0, 1, 1, 2]);
    }

    #[test]
    fn normalizing_nullable_fixed_size_lists_bounds_padding_by_its_null_leg() {
        let width = 1 << 20;
        let element = LogicalType::Scalar(ScalarType::FixedSizeBinary(width)).join(&LogicalType::Scalar(ScalarType::I64)).maybe();
        let list = LogicalType::List { kind: rtdb_types::ListKind::FixedSizeList(1), element: Box::new(element) };
        let value = Value::FixedSizeList(vec![Value::I64(13)]);
        let array = build_array(&list, std::slice::from_ref(&value)).unwrap();
        let legs = UnionFields::try_new([2, 4], [
            Field::new("fixed-size-list-1", array.data_type().clone(), false),
            Field::new("null", DataType::Null, true),
        ]).unwrap();
        let array = UnionArray::try_new(legs, vec![2_i8, 4].into(), Some(vec![0_i32, 0].into()), vec![
            array, Arc::new(arrow_array::NullArray::new(1)),
        ]).unwrap();
        let schema = Arc::new(Schema::new(vec![Field::new("v", array.data_type().clone(), false)]));
        let memory = MemoryBudget::new(1 << 18);
        let batch = OwnedBatch::new(RecordBatch::try_new(schema, vec![Arc::new(array)]).unwrap(), &memory).unwrap();

        let documents = ArrowDocuments::from_batch(&batch, &memory).unwrap().unwrap();

        let (field, array) = document_column(documents.array(), "v").unwrap();
        assert!(matches!(field.data_type(), DataType::FixedSizeList(_, 1)));
        assert_eq!(read_value(field, array.as_ref(), 0).unwrap(), value);
        assert_eq!(read_value(field, array.as_ref(), 1).unwrap(), Value::Null);
        assert!(memory.peak_bytes() < width as usize);
    }

    #[test]
    fn fixed_size_list_ingest_rejects_visible_bottom_children_and_accepts_masked_ones() {
        use arrow_buffer::NullBuffer;
        let ty = LogicalType::List { kind: rtdb_types::ListKind::FixedSizeList(1), element: Box::new(LogicalType::Nothing) }.maybe();
        let field = to_field("items", &ty).unwrap();
        let batch = |nulls| {
            let array = arrow_array::new_empty_array(field.data_type()).to_data().into_builder().len(1)
                .nulls(nulls).child_data(vec![arrow_array::NullArray::new(1).to_data()]).build().unwrap();
            RecordBatch::try_new(Arc::new(Schema::new(vec![field.clone()])), vec![arrow_array::make_array(array)]).unwrap()
        };
        let memory = budget();
        let visible = OwnedBatch::new(batch(None), &memory).unwrap();
        let masked = OwnedBatch::new(batch(Some(NullBuffer::from(vec![false]))), &memory).unwrap();

        let error = ArrowDocuments::from_batch(&visible, &memory).unwrap_err();
        assert_eq!(error.code, "xtdb/zero-leg-read");
        let documents = ArrowDocuments::from_batch(&masked, &memory).unwrap().unwrap();
        assert_eq!(documents.array().column(0).data_type(), &DataType::Null);
    }

    #[test]
    fn fixed_size_list_replay_rejects_visible_bottom_from_a_row_backed_put() {
        let memory = budget();
        let documents = Documents::new(vec![StructFields::from([
            ("_id".into(), Value::I64(1)),
            ("items".into(), Value::FixedSizeList(vec![Value::Undefined])),
        ])], &memory).unwrap();
        let source = SourceTransaction::new(vec![TxOp::PutDocs {
            table: TableRef::new("public", "docs"), documents, valid_from: None, valid_to: None,
        }], SubmitOptions::default(), &memory).unwrap();
        let encoded = source.encode().unwrap();
        let message = SourceMessage::decode(encoded.as_slice()).unwrap().unwrap();

        let error = SourceTransaction::decode(&message, &memory).err().unwrap();
        assert_eq!(error.code, "xtdb/zero-leg-read");
    }

    #[test]
    fn evolving_structs_inside_arrow_lists_replay_columnarly() {
        let memory = budget();
        let put = |id, name: &str, number| {
            let list = Value::List(vec![Value::Struct(StructFields::from([
                (name.into(), Value::Bool(true)), ("number".into(), number),
            ]))]);
            let logical = list.logical_type();
            let fields = vec![Field::new("_id", DataType::Int64, false), to_field("items", &logical).unwrap()];
            let columns = vec![
                Arc::new(arrow_array::Int64Array::from(vec![id])) as ArrayRef,
                build_array(&logical, &[list]).unwrap(),
            ];
            let batch = RecordBatch::try_new(Arc::new(Schema::new(fields)), columns).unwrap();
            let batch = OwnedBatch::new(batch, &memory).unwrap();
            let documents = ArrowDocuments::from_batch(&batch, &memory).unwrap().unwrap();
            TxOp::PutArrowDocs { table: TableRef::new("public", "docs"), documents }
        };
        let source = SourceTransaction::new(vec![
            put(1, "first", Value::I64(7)), put(2, "later", Value::Utf8("text".into())),
        ], SubmitOptions::default(), &memory).unwrap();

        let replay = roundtrip(&source);

        let expected = [(true, false, Value::I64(7)), (false, true, Value::Utf8("text".into()))];
        assert_eq!(replay.operations.len(), expected.len());
        for (operation, (first, later, number)) in replay.operations.iter().zip(expected) {
            let Operation::PutReplayArrow { documents, .. } = operation else {
                panic!("Expected Arrow documents on replay");
            };
            let list = documents.array().column_by_name("items").unwrap().as_any().downcast_ref::<ListArray>().unwrap();
            let item = list.value(0);
            let item = item.as_any().downcast_ref::<StructArray>().unwrap();
            assert_eq!(item.fields().iter().map(|field| field.name().as_str()).collect::<Vec<_>>(), ["first", "number", "later"]);
            assert_eq!(item.column(0).is_valid(0), first);
            assert_eq!(item.column(2).is_valid(0), later);
            assert!(item.column(1).as_any().is::<UnionArray>());
            assert_eq!(read_value(&item.fields()[1], item.column(1).as_ref(), 0).unwrap(), number);
        }
    }

    #[test]
    fn a_nested_struct_key_first_written_after_its_struct_was_undefined_is_nullable() {
        let memory = budget();
        let put = |row: StructFields<Value>| TxOp::PutDocs { table: TableRef::new("public", "docs"), documents: Documents::new(vec![row], &memory).unwrap(), valid_from: None, valid_to: None };
        let quux = StructFields::from([("_id".to_owned(), Value::I64(2)), ("struct".to_owned(), Value::Struct(StructFields::from([
            ("a".to_owned(), Value::Bool(true)),
            ("b".to_owned(), Value::Struct(StructFields::from([("c".to_owned(), Value::Utf8("c".into())), ("d".to_owned(), Value::Utf8("d".into()))]))),
        ])))]);
        let source = SourceTransaction::new(vec![put(StructFields::from([("_id".to_owned(), Value::I64(1))])), put(quux)], SubmitOptions::default(), &memory).unwrap();
        let decoded = roundtrip(&source);
        let Operation::PutReplayArrow { documents, .. } = &decoded.operations[0] else { panic!("put"); };
        let documents = documents.to_rows(true, &memory).unwrap();
        let utf8 = LogicalType::Scalar(ScalarType::Utf8);
        let expected = LogicalType::Struct(StructFields::from([
            ("a".to_owned(), LogicalType::Scalar(ScalarType::Bool)),
            ("b".to_owned(), LogicalType::Struct(StructFields::from([("c".to_owned(), utf8.clone().maybe()), ("d".to_owned(), utf8.maybe())]))),
        ])).maybe();
        assert_eq!(documents.declared_types().unwrap()["struct"], expected);
    }

    #[test]
    fn document_union_keeps_first_seen_column_order_per_table_through_replay() {
        let memory = budget();
        let operation = |table, id, names: &[&str], cells: &[(&str, Value)]| {
            let mut row = StructFields::from([("_id".to_owned(), Value::I64(id))]);
            row.extend(cells.iter().map(|(name, value)| ((*name).to_owned(), value.clone())));
            let columns = names.iter().map(|name| (*name).to_owned()).collect();
            TxOp::PutDocs {
                table: TableRef::new("public", table),
                documents: Documents::with_column_order(vec![row.into()], columns, &memory).unwrap(),
                valid_from: None,
                valid_to: None,
            }
        };
        let source = SourceTransaction::new(vec![
            operation("a", 1, &["z", "_id", "n"], &[("z", Value::Utf8("last".into())), ("n", Value::Null)]),
            operation("b", 2, &["_id", "e"], &[]),
            operation("a", 3, &["b", "_id", "z"], &[("b", Value::Bool(true)), ("z", Value::I64(7))]),
        ], SubmitOptions::default(), &memory).unwrap();
        let decoded = roundtrip(&source);
        let replayed = roundtrip(&decoded);
        for tx in [&decoded, &replayed] {
            let mut operations = tx.operations.iter();
            let Operation::PutReplayArrow { documents, .. } = operations.next().unwrap() else { panic!("first put"); };
            let documents = documents.to_rows(true, &memory).unwrap();
            assert_eq!(documents.columns(), ["z", "_id", "n", "b"]);
            assert_eq!(documents.rows()[0]["z"], Value::Utf8("last".into()));
            assert_eq!(documents.declared_types().unwrap()["n"], LogicalType::Null);
            assert!(!documents.rows()[0].contains_key("n"));
            let Operation::PutReplayArrow { documents, .. } = operations.next().unwrap() else { panic!("second put"); };
            let documents = documents.to_rows(true, &memory).unwrap();
            assert_eq!(documents.columns(), ["_id", "e"]);
            assert_eq!(documents.declared_types().unwrap()["e"], LogicalType::Null);
            assert!(!documents.rows()[0].contains_key("e"));
            let Operation::PutReplayArrow { documents, .. } = operations.next().unwrap() else { panic!("third put"); };
            let documents = documents.to_rows(true, &memory).unwrap();
            assert_eq!(documents.columns(), ["z", "_id", "n", "b"]);
            assert_eq!(documents.rows()[0]["z"], Value::I64(7));
            assert_eq!(documents.rows()[0]["b"], Value::Bool(true));
        }
    }

    #[test]
    fn source_writes_sparse_document_columns_with_one_column_of_builder_scratch() {
        let memory = MemoryBudget::new(1 << 27);
        let rows = (0..100).map(|id| {
            let mut row = StructFields::from([("_id".into(), Value::I64(id))]);
            for index in 0..10 {
                row.insert(format!("field_{id}_{index}"), Value::Utf8("value".into()));
            }
            row
        }).collect();
        let tx = SourceTransaction::new(vec![TxOp::PutDocs {
            table: TableRef::new("public", "wide_docs"),
            documents: Documents::new(rows, &memory).unwrap(),
            valid_from: None,
            valid_to: None,
        }], SubmitOptions::default(), &memory).unwrap();
        let encoded = tx.encode().unwrap();
        assert!(encoded.as_slice().len() < 1 << 20);
        assert!(memory.peak_bytes() < 1 << 27);
    }

    #[test]
    fn source_reads_sparse_documents_with_one_row_of_decode_scratch() {
        let memory = MemoryBudget::new(1 << 25);
        let operations = (0..100).map(|id| {
            let mut row = StructFields::from([("_id".into(), Value::I64(id))]);
            for index in 0..10 {
                row.insert(format!("field_{id}_{index}"), Value::Utf8(format!("value_{id}_{index}")));
            }
            TxOp::PutDocs {
                table: TableRef::new("public", "wide_docs"),
                documents: Documents::new(vec![row.into()], &memory).unwrap(),
                valid_from: None,
                valid_to: None,
            }
        }).collect();
        let source = SourceTransaction::new(operations, SubmitOptions::default(), &memory).unwrap();
        let encoded = source.encode().unwrap();
        drop(source);
        let message = SourceMessage::decode(encoded.as_slice()).unwrap().unwrap();
        let decoded = SourceTransaction::decode(&message, &memory).unwrap();
        assert_eq!(decoded.operations.len(), 100);
        let Operation::PutReplayArrow { documents, .. } = &decoded.operations[99] else { panic!("Expected columnar decoded put"); };
        let documents = documents.to_rows(true, &memory).unwrap();
        assert_eq!(documents.columns().len(), 1001);
        assert_eq!(documents.rows()[0]["field_99_9"], Value::Utf8("value_99_9".into()));
        assert_eq!(documents.declared_types().unwrap()["field_0_0"], LogicalType::Scalar(ScalarType::Utf8).maybe());
        assert!(!documents.rows()[0].contains_key("field_0_0"));
    }

    #[test]
    fn ordered_documents_reject_duplicate_columns_and_unrepresented_fields() {
        let memory = budget();
        for names in [vec!["a", "a"], vec!["b"]] {
            let rows = vec![StructFields::from([("a".into(), Value::I64(1))])];
            let columns = names.into_iter().map(str::to_owned).collect();
            let error = Documents::with_column_order(rows, columns, &memory).unwrap_err();
            assert_eq!(error.code, "rtdb.tx/document-schema");
            assert_eq!(memory.used_bytes(), 0);
        }
    }

    #[test]
    fn replay_iids_read_only_the_selected_list_position() {
        use arrow_buffer::NullBuffer;

        let first = [1; 16];
        let second = [2; 16];
        let third = [3; 16];
        let elements = Arc::new(Field::new("item", DataType::FixedSizeBinary(16), true));
        let binary = FixedSizeBinaryArray::try_from_sparse_iter_with_size(
            [Some(first.as_slice()), Some(second.as_slice()), Some(third.as_slice()), None].into_iter(), 16,
        ).unwrap();
        let lists = ListArray::new(elements, OffsetBuffer::new(vec![0, 1, 1, 1, 3, 4].into()),
            Arc::new(binary), Some(NullBuffer::from(vec![true, false, true, true, true])));
        let fields = vec![Field::new("iids", lists.data_type().clone(), true)].into();
        let body = StructArray::new(fields, vec![Arc::new(lists)], None);
        let body = body.slice(1, 4);
        assert_eq!(read_iids(&body, 0).unwrap(), None);
        assert_eq!(read_iids(&body, 1).unwrap(), Some(vec![]));
        assert_eq!(read_iids(&body, 2).unwrap(), Some(vec![Iid(second), Iid(third)]));
        assert!(read_iids(&body, 3).unwrap_err().message.as_deref().is_some_and(|message| message.contains("IID must be fixed-size binary")));
    }

    #[test]
    fn replay_keeps_explicit_put_and_patch_iids_even_when_the_document_id_hash_differs() {
        let memory = budget();
        let documents = Documents::new(vec![StructFields::from([
            ("_id".into(), Value::Utf8("different-id".into())),
            ("tags".into(), Value::Set(vec![Value::Utf8("open".into()), Value::Null])),
            ("optional".into(), Value::Null),
        ])], &memory).unwrap();
        let table = TableRef::new("public", "docs");
        let iid = Iid([42; 16]);
        assert_ne!(iid, EntityId::String("different-id".into()).iid());
        let source = SourceTransaction::from_operations(vec![
            Operation::PutIids { table: table.clone(), documents: documents.clone(), iids: vec![iid], valid_from: Some(Timestamp(10)), valid_to: Some(Timestamp(20)) },
            Operation::PatchIids { table, documents, iids: vec![iid], valid_from: Some(Timestamp(30)), valid_to: None },
        ], SubmitOptions::default(), &memory).unwrap();
        let decoded = roundtrip(&source);
        assert!(matches!(&decoded.operations[0], Operation::PutReplayArrow { iids: Some(iids), valid_from: Some(Timestamp(10)), valid_to: Some(Timestamp(20)), .. } if iids == &vec![iid]));
        assert!(matches!(&decoded.operations[1], Operation::PatchArrowIids { documents, iids, valid_from: Some(Timestamp(30)), valid_to: None, .. }
            if iids == &vec![iid] && documents.requires_legacy_receipt(true)));
        assert_eq!(decoded.body_digest().unwrap(), source.body_digest().unwrap());
    }

    #[test]
    fn compatible_insert_replays_arrow_documents_and_keeps_the_legacy_receipt_digest() {
        let memory = budget();
        let documents = Documents::new(vec![
            StructFields::from([("_id".into(), Value::I64(1)), ("value".into(), Value::I64(10))]),
            StructFields::from([("_id".into(), Value::I64(2))]),
        ], &memory).unwrap();
        let iids = vec![Iid([1; 16]), Iid([2; 16])];
        let table = TableRef::new("public", "docs");
        let source = SourceTransaction::from_operations(vec![
            Operation::InsertIids { table: table.clone(), documents, iids: iids.clone(), valid_from: Some(Timestamp(10)), valid_to: None },
        ], SubmitOptions::default(), &memory).unwrap();
        let decoded = roundtrip(&source);
        let Operation::InsertArrowIids { documents, iids: decoded_iids, valid_from, valid_to, .. } = &decoded.operations[0]
        else { panic!("expected Arrow insert"); };
        assert_eq!(decoded_iids, &iids);
        assert_eq!(*valid_from, Some(Timestamp(10)));
        assert_eq!(documents.array().len(), 2);
        let legacy = SourceTransaction::from_operations(vec![
            Operation::InsertIids { table, documents: documents.to_rows(true, &memory).unwrap(), iids, valid_from: *valid_from, valid_to: *valid_to },
        ], SubmitOptions::default(), &memory).unwrap();
        assert_eq!(decoded.body_digest().unwrap(), legacy.body_digest().unwrap());
        assert!(matches!(&roundtrip(&decoded).operations[0], Operation::InsertArrowIids { .. }));
    }

    #[test]
    fn legacy_put_without_iid_vector_keeps_arrow_documents_and_the_receipt_digest() {
        let memory = budget();
        let table = TableRef::new("public", "docs");
        let id = Value::Utf8("legacy-put".into());
        let source = SourceTransaction::new(vec![TxOp::PutDocs {
            table,
            documents: Documents::new(vec![StructFields::from([("_id".into(), id.clone()), ("v".into(), Value::I64(2))])], &memory).unwrap(),
            valid_from: None,
            valid_to: None,
        }], SubmitOptions::default(), &memory).unwrap();
        let mut message = SourceMessage::decode(source.encode().unwrap().as_slice()).unwrap().unwrap();
        let SourceMessage::Protobuf(SourceBody::Tx(tx)) = &mut message else { panic!("Expected a transaction envelope"); };
        let pages = crate::ipc::decode(tx.tx_ops.as_ref().unwrap(), &memory).unwrap();
        let batch = pages[0].batch();
        let operations = batch.column(0).as_union();
        let tag = operations.type_id(0);
        let put = operations.child(tag).as_struct();
        let fields = put.fields().iter().map(|field| {
            if field.name() == "iids" { Arc::new(Field::new("iids", DataType::Null, true)) }
            else { Arc::clone(field) }
        }).collect::<Vec<_>>();
        let columns = put.fields().iter().zip(put.columns()).map(|(field, column)| {
            if field.name() == "iids" { Arc::new(arrow_array::NullArray::new(1)) as ArrayRef }
            else { Arc::clone(column) }
        }).collect::<Vec<_>>();
        let body = Arc::new(StructArray::new(fields.into(), columns, None));
        let fields = UnionFields::try_new([tag], [Field::new("put-docs", body.data_type().clone(), false)]).unwrap();
        let operations = Arc::new(UnionArray::try_new(fields, vec![tag].into(), Some(vec![0i32].into()), vec![body]).unwrap());
        let schema = Arc::new(Schema::new(vec![batch.schema_ref().field(0).clone().with_data_type(operations.data_type().clone())]));
        let legacy = RecordBatch::try_new(Arc::clone(&schema), vec![operations]).unwrap();
        tx.tx_ops = Some(crate::ipc::encode(&schema, [&legacy], &memory).unwrap().as_slice().to_vec());
        let decoded = SourceTransaction::decode(&message, &memory).unwrap();
        let [Operation::PutReplayArrow { documents, iids: None, .. }] = decoded.operations.as_slice() else {
            panic!("Expected legacy Arrow put");
        };
        let reader = documents.id_reader().unwrap();
        assert_eq!(documents.iid(0, reader.as_ref(), &memory).unwrap(), id.entity_id().unwrap().iid());
        assert_eq!(decoded.body_digest().unwrap(), source.body_digest().unwrap());
    }

    #[test]
    fn receipt_reader_can_charge_and_preserve_an_undefined_document_cell() {
        let array = arrow_array::NullArray::new(1);
        let field = Field::new("omitted", DataType::Null, false);
        let cell = DocumentCell::Arrow { field: &field, column: &array, logical: &LogicalType::Nothing, row: 0 };
        assert_eq!(cell.bound().unwrap(), 0);
        assert_eq!(*cell.value().unwrap(), Value::Undefined);
    }

    #[test]
    fn absent_empty_and_single_row_zero_column_sql_arguments_remain_distinct() {
        let memory = budget();
        let source = SourceTransaction::new(vec![
            TxOp::Sql { sql: "SELECT 1".into(), args: None },
            TxOp::Sql { sql: "SELECT 1".into(), args: Some(SqlArguments::new(vec![], vec![], &memory).unwrap()) },
            TxOp::Sql { sql: "SELECT 1".into(), args: Some(SqlArguments::new(vec![], vec![vec![]], &memory).unwrap()) },
        ], SubmitOptions::default(), &memory).unwrap();
        let decoded = roundtrip(&source);
        assert!(matches!(&decoded.operations[0], Operation::Public(TxOp::Sql { args: None, .. })));
        assert!(matches!(&decoded.operations[1], Operation::Public(TxOp::Sql { args: Some(args), .. }) if args.row_count() == 0));
        assert!(matches!(&decoded.operations[2], Operation::Public(TxOp::Sql { args: Some(args), .. }) if args.row_count() == 1 && args.types().is_empty()));
    }

    #[test]
    fn raw_decimal_sql_arguments_retain_metadata_and_coefficients_through_the_source_codec() {
        let memory = budget();
        for precision in [i32::MIN, -1, 0, 1, 256, i32::MAX] {
            let value = Value::Decimal(rtdb_types::DecimalValue::new(18i128.to_be_bytes().to_vec(), precision, 129, 128).unwrap());
            let args = SqlArguments::new(vec![value.logical_type()], vec![vec![value.clone()]], &memory).unwrap();
            let source = SourceTransaction::new(vec![TxOp::Sql { sql: "INSERT INTO docs (_id, v) VALUES (2, ?)".into(), args: Some(args) }], SubmitOptions::default(), &memory).unwrap();
            let decoded = roundtrip(&source);
            let Operation::Public(TxOp::Sql { args: Some(args), .. }) = &decoded.operations[0] else { panic!("Expected SQL arguments") };
            assert_eq!(args.types(), &[value.logical_type()]);
            assert_eq!(args.rows().map(|row| row.unwrap().to_vec()).collect::<Vec<_>>(), vec![std::slice::from_ref(&value)]);
        }
    }

    #[test]
    fn empty_delete_and_erase_are_omitted_but_forbidden_tables_are_checked_first() {
        let memory = budget();
        let ids = EntityIds::new(vec![], &memory).unwrap();
        let source = SourceTransaction::new(vec![
            TxOp::DeleteDocs { table: TableRef::new("public", "docs"), ids: ids.clone(), valid_from: None, valid_to: None },
            TxOp::EraseDocs { table: TableRef::new("public", "docs"), ids: ids.clone() },
        ], SubmitOptions::default(), &memory).unwrap();
        assert!(roundtrip(&source).operations.is_empty());
        let source = SourceTransaction::new(vec![TxOp::EraseDocs { table: TableRef::new("xt", "txs"), ids }], SubmitOptions::default(), &memory).unwrap();
        let error = source.encode().unwrap_err();
        assert_eq!(error.code, "xtdb/forbidden-table");
        assert_eq!(error.message.as_deref(), Some("Cannot write to table: xt.txs"));
        assert_eq!(error.context["schema"], "xt");
        assert_eq!(error.context["table"], "txs");
    }

    #[test]
    fn empty_metadata_bytes_are_absent_and_present_metadata_keeps_its_types() {
        let memory = budget();
        let value = Value::Struct(StructFields::from([("source".into(), Value::Utf8("client".into())), ("attempt".into(), Value::I64(2))]));
        let source = SourceTransaction::new(vec![], SubmitOptions { user_metadata: Some(UserMetadata::new(value.clone(), &memory).unwrap()), ..SubmitOptions::default() }, &memory).unwrap();
        assert_eq!(roundtrip(&source).options.user_metadata.unwrap().value(), &value);
        let mut message = SourceMessage::decode(source.encode().unwrap().as_slice()).unwrap().unwrap();
        let SourceMessage::Protobuf(SourceBody::Tx(tx)) = &mut message else { panic!("wrong envelope") };
        tx.user_metadata = Some(vec![]);
        assert!(SourceTransaction::decode(&message, &memory).unwrap().options.user_metadata.is_none());
    }
}


fn capacity_condition_bytes(relation: &rtdb_types::OwnedRelation, column_bytes: usize, prefix_bytes: usize) -> Result<usize> {
    relation.name().len().checked_add(column_bytes)
        .and_then(|size| size.checked_add(prefix_bytes)).ok_or_else(overflow)
}

fn range_evidence_bytes(relation: &rtdb_types::OwnedRelation, fixed: &Option<Arc<rtdb_types::FixedRangeEvidence>>) -> usize {
    relation.name().len() + fixed.as_ref().map_or(0, |fixed| fixed.retained_bytes())
}
