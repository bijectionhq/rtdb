use rtdb_types::{HOSTED_INSTANT_MICROS, StructFields};
use std::{borrow::Cow, collections::BTreeMap};
use std::sync::Arc;

use arrow_array::{Array, ArrayRef, BinaryArray, FixedSizeBinaryArray, ListArray, RecordBatch, StructArray, UnionArray, cast::AsArray};
use arrow_buffer::{Buffer, NullBuffer, NullBufferBuilder, OffsetBuffer, ScalarBuffer};
use arrow_schema::{DataType, Field, Schema, UnionFields, UnionMode};
use bytes::{BufMut, Bytes};
use prost::{Message, encoding::{WireType, encode_key, encode_varint, encoded_len_varint, key_len}};
use rtdb_columnar::{ArrowCell, BatchBuilder, MemoryBudget, MemoryReservation, OwnedBatch, build_array, copy_array, estimate_builder_bytes, estimate_builder_bytes_refs, estimate_read_value_bytes, from_field, read_value, struct_field_cell, to_field};
use rtdb_format::{PROTOBUF_LOG_HEADER, envelope::{SourceBody, SourceMessage}};
use rtdb_types::{Anomaly, EntityId, Iid, LogicalType, OrderedSet, PlacedAttempt, PlacedAttemptSeed, Result, ScalarType, SourcePosition, TableRef, TimeUnit, Timestamp, Value};

use crate::{ArrowDocumentWrite, ArrowDocuments, Documents, EntityIds, OwnedPeriodEffect, OwnedRelativePeriod, ReplaySubmission, RetainedBytes, SqlArguments, SubmitOptions, TxOp, UserMetadata};
use crate::bounded_retry::{BOUNDED_RETRY_METADATA_KEY, BoundedRetry};
use crate::values::{DecodedDocumentLayout, LegacyOwnedPresence, contains_fixed_size_list};
use crate::values::arguments::{canonical_float_field, compatible_float_cell, float_field};

pub(crate) mod canonical;

const LEGACY_REPLAY_ABORT: &str = "ASSERT FALSE";
const SOURCE_TX_TAG: u32 = 6;
const TX_OPS_TAG: u32 = 1;
// Keep the IPC start on an eight-byte boundary within the allocation for retained replay.
const SOURCE_PREFIX_SPACE: usize = (1 + key_len(SOURCE_TX_TAG) + key_len(TX_OPS_TAG)
    + 2 * encoded_len_varint(u64::MAX)).next_multiple_of(8);

#[derive(Clone, Debug)]
pub(crate) enum Operation {
    Public(TxOp),
    OwnedArrowIids { create_only: bool, relation: rtdb_types::OwnedRelation, documents: ArrowDocuments, iids: Vec<Iid>, presence: Option<Vec<std::collections::BTreeSet<String>>>, valid_from: Option<Timestamp>, valid_to: Option<Timestamp> },
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
            Self::Public(TxOp::OwnedDeclareRelation { relation, .. } | TxOp::OwnedDeclarePath { relation, .. } | TxOp::OwnedDeclareSpatialPath { relation, .. } | TxOp::OwnedDeclareEncodedSpatialPath { relation, .. } | TxOp::OwnedRequireSpatialPath { relation, .. } | TxOp::OwnedDropSpatialPath { relation, .. }
                | TxOp::OwnedRetireRelation { relation, .. } | TxOp::OwnedDeclareSearchPath { relation, .. } | TxOp::OwnedDeclareUniquePath { relation, .. } | TxOp::OwnedDeclareVectorPath { relation, .. }
                | TxOp::OwnedDeclareFixedRange { relation, .. } | TxOp::OwnedDeclareCapacity { relation, .. } | TxOp::OwnedCheckCapacity { relation, .. } | TxOp::OwnedCheckRange { relation, .. } | TxOp::OwnedCheck { relation, .. } | TxOp::OwnedCheckWorldPurge { relation, .. }
                | TxOp::OwnedErase { relation, .. } | TxOp::OwnedReplayResidue { relation, .. } | TxOp::OwnedSweepExpired { relation, .. } | TxOp::OwnedDelete { relation, .. } | TxOp::OwnedInsertDocs { relation, .. }
                | TxOp::OwnedReplaceDocs { relation, .. } | TxOp::OwnedArrowDocs { relation, .. } | TxOp::OwnedRelativePeriod { relation, .. }) | Self::OwnedWriteIids { relation, .. } | Self::OwnedArrowIids { relation, .. } => Some(relation),
            _ => None,
        }
    }

    pub(crate) fn direct_archive_relation(&self) -> Option<&rtdb_types::OwnedRelation> {
        match self {
            Self::Public(TxOp::OwnedDelete { relation, .. } | TxOp::OwnedErase { relation, .. }
                | TxOp::OwnedReplayResidue { relation, .. }) => Some(relation),
            operation => operation.owned_documents().map(|(relation, _)| relation),
        }
    }

    pub(crate) fn owned_documents(&self) -> Option<(&rtdb_types::OwnedRelation, usize)> {
        match self {
            Self::Public(TxOp::OwnedInsertDocs { relation, documents, .. } | TxOp::OwnedReplaceDocs { relation, documents, .. })
            | Self::OwnedWriteIids { relation, documents, .. } => Some((relation, documents.rows().len())),
            Self::Public(TxOp::OwnedArrowDocs { relation, documents, .. })
            | Self::OwnedArrowIids { relation, documents, .. } => Some((relation, documents.array().len())),
            _ => None,
        }
    }

    pub(crate) fn is_owned(&self) -> bool {
        match self { Self::Public(operation) => operation.is_owned(), Self::OwnedWriteIids { .. } | Self::OwnedArrowIids { .. } => true, _ => false }
    }
}

pub struct SourceTransaction {
    pub(crate) operations: Vec<Operation>,
    pub options: SubmitOptions,
    pub(crate) guard: Option<SourceGuard>,
    placed_seed: Option<PlacedAttemptSeed>,
    pub(crate) prepared_landing: Option<crate::resolver::PreparedLanding>,
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
    pub(crate) fn place_owned_periods(&mut self, ordered: Timestamp, placed: bool) -> Result<()> {
        for operation in &mut self.operations {
            let Operation::Public(TxOp::OwnedRelativePeriod { relation, period, .. }) = operation else {
                continue;
            };
            let invalid = || Anomaly::incorrect("rtdb.owned/validity-policy", "Invalid acceptance-relative Root period");
            if !placed || !relation.is_root_data() {
                return Err(invalid());
            }
            let (from, to) = period.at(ordered).ok_or_else(invalid)?;
            if !HOSTED_INSTANT_MICROS.contains(&from.0) || !HOSTED_INSTANT_MICROS.contains(&to.0) {
                return Err(invalid());
            }
            let relative = std::mem::replace(operation, Operation::Removed(String::new()));
            let Operation::Public(TxOp::OwnedRelativePeriod { relation, effect, .. }) = relative else {
                unreachable!("the source owns a relative period")
            };
            let (valid_from, valid_to) = (Some(from), Some(to));
            *operation = Operation::Public(match effect {
                OwnedPeriodEffect::Create(documents) => TxOp::OwnedInsertDocs { relation, documents, valid_from, valid_to },
                OwnedPeriodEffect::Replace(documents) => TxOp::OwnedReplaceDocs { relation, documents, valid_from, valid_to },
                OwnedPeriodEffect::Arrow { documents, create_only } => TxOp::OwnedArrowDocs { relation, documents, create_only, valid_from, valid_to },
                OwnedPeriodEffect::Delete(id) => TxOp::OwnedDelete { relation, id, valid_from, valid_to },
            });
        }
        Ok(())
    }

    pub(crate) fn place_proposal_keys(&mut self, ordered: Timestamp, placed: bool) -> Result<()> {
        use crate::resolver::ordered_micros;
        for operation in &mut self.operations {
            let (relation, documents) = match operation {
                Operation::Public(TxOp::OwnedInsertDocs { relation, documents, .. }
                    | TxOp::OwnedReplaceDocs { relation, documents, .. })
                | Operation::OwnedWriteIids { relation, documents, .. } => (relation, documents),
                _ => continue,
            };
            if relation.world().is_none() || !documents.columns().iter().any(|name| name == ordered_micros::FIELD) {
                continue;
            }
            if !placed {
                return Err(ordered_micros::invalid());
            }
            *documents = ordered_micros::fill_proposal_keys(documents, ordered, &self.memory)?;
        }
        Ok(())
    }

    pub fn requires_durable_predecessor(&self) -> bool {
        crate::resolver::is_prepared_landing(&self.operations) || self.uses_staging_deadline()
            || self.operations.iter().any(|operation| matches!(operation,
                Operation::Public(TxOp::HostedControl(control)) if control.is_assertion_family_marker())
                || matches!(operation, Operation::Public(TxOp::OwnedAdoptRelation { .. })))
    }

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
                Operation::Public(TxOp::OwnedDeclareSearchPath { relation, name, column, .. } | TxOp::OwnedDeclareUniquePath { relation, name, column }
                    | TxOp::OwnedDeclareVectorPath { relation, name, column, .. }) =>
                    (owned_path_bytes(relation, name, std::slice::from_ref(column))?, None, 0),
                Operation::Public(TxOp::OwnedCheckCapacity { relation, column, prefix, .. }) =>
                    (capacity_condition_bytes(relation, column.capacity(), prefix.capacity())?, None, 0),
                Operation::Public(TxOp::OwnedDeclareCapacity { relation, column, when, .. }) =>
                    (capacity_condition_bytes(relation, column.capacity(), when.capacity())?, None, 0),
                Operation::Public(TxOp::OwnedDropSpatialPath { relation, name }) =>
                    (relation.name().len() + name.name().len(), None, 0),
                Operation::Public(TxOp::OwnedRetireRelation { relation, .. }) => (relation.name().len(), None, 0),
                Operation::Public(TxOp::OwnedAdoptRelation { source, destination, .. }) =>
                    (source.name().len() + destination.name().len(), None, 0),
                Operation::Public(TxOp::OwnedDeclareRelation { relation, schema }) =>
                    (relation.name().len().checked_add(schema.retained_bytes()).ok_or_else(overflow)?, None, 0),
                Operation::Public(TxOp::OwnedDeclarePath { relation, name, columns } | TxOp::OwnedDeclareSpatialPath { relation, name, columns } | TxOp::OwnedDeclareEncodedSpatialPath { relation, name, columns, .. } | TxOp::OwnedRequireSpatialPath { relation, name, columns, .. }) =>
                    (owned_path_bytes(relation, name, columns)?, None, 0),
                Operation::Public(TxOp::HostedControl(control)) => (control.retained_bytes()?, None, 0),
                Operation::Public(TxOp::OwnedForkWorld { record, attempt, .. }) => (Value::document_retained_bytes(record)?.saturating_add(attempt.len()), None, 128),
                Operation::Public(TxOp::OwnedPublishRoot { attempt, .. } | TxOp::OwnedPublishWorld { attempt, .. }) => (attempt.len(), None, 128),
                Operation::Public(TxOp::OwnedCheckWorldPurge { relation, .. }) => (relation.name().len(), None, 0),
                Operation::Public(TxOp::OwnedDeadline { .. } | TxOp::OwnedClockSlot { .. } | TxOp::OwnedCheckWorldScope { .. } | TxOp::HostedConstraintRefusal) => (0, None, 0),
                Operation::Public(TxOp::OwnedDeclareFixedRange { relation, rule }) =>
                    (relation.name().len() + rule.path.capacity() + rule.column.capacity(), None, 0),
                Operation::Public(TxOp::OwnedCheckRange { relation, region, .. }) =>
                    (range_evidence_bytes(relation, region), None, 0),
                Operation::Public(TxOp::OwnedCheck { relation, id, .. } | TxOp::OwnedDelete { relation, id, .. } | TxOp::OwnedErase { relation, id } | TxOp::OwnedReplayResidue { relation, id, .. } | TxOp::OwnedSweepExpired { relation, id, .. }) =>
                    (relation.name().len().checked_add(id.as_value().retained_bytes()?).ok_or_else(overflow)?, None, 0),
                Operation::Public(TxOp::OwnedInsertDocs { relation, .. } | TxOp::OwnedReplaceDocs { relation, .. } | TxOp::OwnedArrowDocs { relation, .. }) => (relation.name().len(), None, 0),
                Operation::Public(TxOp::OwnedRelativePeriod { relation, effect, .. }) => {
                    let identity = match effect {
                        OwnedPeriodEffect::Delete(id) => id.as_value().retained_bytes()?,
                        OwnedPeriodEffect::Create(_) | OwnedPeriodEffect::Replace(_) | OwnedPeriodEffect::Arrow { .. } => 0,
                    };
                    (relation.name().len().checked_add(identity).ok_or_else(overflow)?, None, 0)
                },
                Operation::OwnedArrowIids { relation, iids, presence, .. } => {
                    let inventory = presence.iter().flatten().flatten().try_fold(0usize, |bytes, name| bytes.checked_add(name.capacity() + 128).ok_or_else(overflow))?;
                    (relation.name().len(), None, iids.capacity().checked_mul(16).and_then(|bytes| bytes.checked_add(inventory)).ok_or_else(overflow)?)
                }
                Operation::OwnedWriteIids { relation, iids, .. } => (relation.name().len(), None, iids.capacity().checked_mul(16).ok_or_else(overflow)?),
                Operation::Public(TxOp::Sql { sql, .. } | TxOp::SqlBytes { sql, .. }) => (sql.capacity(), None, 0),
                Operation::Public(TxOp::InsertDocs { table, .. } | TxOp::PutDocs { table, .. } | TxOp::PutArrowDocs { table, .. } | TxOp::WriteArrowDocs { table, .. } | TxOp::PatchDocs { table, .. } | TxOp::DeleteDocs { table, .. } | TxOp::EraseDocs { table, .. }) => (0, Some(table), 0),
                Operation::InsertIids { table, iids, .. } | Operation::InsertArrowIids { table, iids, .. } | Operation::PutIids { table, iids, .. } | Operation::PatchIids { table, iids, .. } | Operation::PatchArrowIids { table, iids, .. } => (0, Some(table), iids.capacity().checked_mul(16).ok_or_else(overflow)?),
                Operation::PutReplayArrow { table, iids, .. } => {
                    (0, Some(table), iids.as_ref().map_or(0, Vec::capacity).checked_mul(16).ok_or_else(overflow)?)
                }
                Operation::Removed(name) => (name.capacity(), None, 0),
            };
            bytes = bytes.checked_add(text).and_then(|size| size.checked_add(ids)).and_then(|size| size.checked_add(table.map_or(0, |table| table.schema.capacity() + table.table.capacity()))).and_then(|size| size.checked_add(256)).ok_or_else(overflow)?;
        }
        let reservation = memory.reserve(bytes)?;
        Ok(Self { operations, options, guard: None, placed_seed: None, prepared_landing: None, memory: Arc::clone(memory), _reservation: reservation })
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

    pub fn placed_attempt(&self, position: SourcePosition) -> Result<Option<PlacedAttempt>> {
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
                let bytes = tx.tx_ops.as_ref().ok_or_else(|| invalid("Transaction has no operations stream"))?;
                let options = SubmitOptions { default_tz: tx.default_tz.clone().ok_or_else(|| invalid("Transaction has no default timezone"))?, user: tx.user.clone(), user_metadata: tx.user_metadata.as_ref().filter(|bytes| !bytes.is_empty()).map(|bytes| decode_metadata(bytes, memory)).transpose()? };
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
            if !matches!(optional_cell(first.batch(), "system-time", 0)?, Value::Null | Value::Undefined) {
                return Err(invalid("The source transaction cannot supply system time"));
            }
            options.user = optional_string(optional_cell(first.batch(), "user", 0)?)?;
            let metadata = optional_cell(first.batch(), "user-metadata", 0)?;
            if !matches!(metadata, Value::Null | Value::Undefined) { options.user_metadata = Some(UserMetadata::new(metadata, memory)?); }
        }
        let mut operations = decode_operation_pages(&pages, legacy, memory, parsed_fields)?;
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
        let batch = self.operation_batch(None, SourcePurpose::Receipt)?;
        crate::ipc::hash_batch(batch.batch(), &self.memory, &mut hash)?;
        let timezone = self.options.default_tz.as_bytes();
        hash.update((timezone.len() as u64).to_be_bytes());
        hash.update(timezone);
        if let Some(Value::Struct(fields)) = self.options.user_metadata.as_ref().map(UserMetadata::value) {
            let mut rest = fields.clone();
            rest.remove("idempotency_key");
            if matches!(self.guard, Some(SourceGuard::Bounded(_))) { rest.remove(BOUNDED_RETRY_METADATA_KEY); }
            let metadata = UserMetadata::new(Value::Struct(rest), &self.memory)?;
            let batch = metadata_batch(&metadata, &self.memory)?;
            crate::ipc::hash_batch(batch.batch(), &self.memory, &mut hash)?;
        }
        Ok(hash.finalize().iter().map(|byte| format!("{byte:02x}")).collect())
    }

    // arrow-flow-scope: rtdb.proposal.landing
    pub(crate) fn landing_footprint(&self) -> Result<Self> {
        use rtdb_columnar::arrow_flow::{self, Measures};

        let flow = arrow_flow::sample();
        let batch = self.operation_batch(None, SourcePurpose::Log)?;
        let schema = batch.batch().schema_ref();
        let schema_bytes = rtdb_format::ipc::schema_translation_bound(schema)?;
        let decode = |pages: &[OwnedBatch]| {
            let mut scratch = self.memory.reserve(4096)?;
            let scratch = Arc::get_mut(&mut scratch).expect("source decoding owns its reservation");
            let operations = decode_operation_pages(pages, false, &self.memory, scratch)?;
            Self::from_operations(operations, self.options.clone(), &self.memory)
        };
        let mut masked = false;
        let mismatch = schema.fields().iter().zip(batch.batch().columns())
            .find_map(|(field, column)| landing_ipc_mismatch(field, column.as_ref(), &mut masked));
        if mismatch.is_none() {
            let _schema = self.memory.reserve(schema_bytes)?;
            if masked {
                // arrow-flow: rtdb.proposal.landing/masked operation-batch operations materialize java-v5 required normalize_java_stream_batch | Normalize masked children and read operations directly from Arrow.
                if let Ok(normalized) = rtdb_format::ipc::normalize_java_stream_batch(batch.batch()) {
                    let normalized = OwnedBatch::with_reservation(normalized, Arc::clone(batch.reservation()))?;
                    if let Some(flow) = flow {
                        flow.record("rtdb.proposal.landing/masked", Measures {
                            layout: Some("masked-children"),
                            rows: Some(normalized.batch().num_rows()),
                            reservation_bytes: Some(normalized.reservation().bytes()),
                            ..Measures::default()
                        });
                    }
                    return decode(std::slice::from_ref(&normalized));
                }
            } else {
                // arrow-flow: rtdb.proposal.landing/compatible operation-batch operations materialize java-v5 required adapt_stream_batch | Read operations from a compatible in-memory layout.
                let batch = match rtdb_format::ipc::adapt_stream_batch(batch.batch())? {
                    Some(adapted) => OwnedBatch::with_reservation(adapted, Arc::clone(batch.reservation()))?,
                    None => batch,
                };
                if let Some(flow) = flow {
                    flow.record("rtdb.proposal.landing/compatible", Measures {
                        layout: Some("compatible"),
                        rows: Some(batch.batch().num_rows()),
                        reservation_bytes: Some(batch.reservation().bytes()),
                        ..Measures::default()
                    });
                }
                return decode(std::slice::from_ref(&batch));
            }
        }

        // arrow-flow: rtdb.proposal.landing/fallback-encode operation-batch temporary-ipc encode java-v5 candidate ipc::encode | The in-memory normalization cases declined. The temporary stream exists for the decoder below.
        let footprint = crate::ipc::encode(schema, [batch.batch()], &self.memory)?;
        let layout = mismatch.unwrap_or("masked-normalization-declined");
        if let Some(flow) = flow {
            flow.record("rtdb.proposal.landing/fallback-encode", Measures {
                layout: Some(layout),
                rows: Some(batch.batch().num_rows()),
                encoded_bytes: Some(footprint.as_slice().len()),
                reservation_bytes: Some(footprint.reservation_bytes()),
                output_buffer: Some(1),
                ..Measures::default()
            });
        }
        drop(batch);
        // arrow-flow: rtdb.proposal.landing/fallback-decode temporary-ipc operations decode java-v5 candidate ipc::decode | Decode the temporary stream to preserve landing normalization.
        let pages = crate::ipc::decode(&footprint.into_bytes(), &self.memory)?;
        if let Some(flow) = flow {
            flow.record("rtdb.proposal.landing/fallback-decode", Measures {
                layout: Some(layout),
                rows: Some(pages.iter().map(|page| page.batch().num_rows()).sum()),
                input_buffer: Some(1),
                ..Measures::default()
            });
        }
        decode(&pages)
    }
    // arrow-flow-end

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
        let mut output = crate::ipc::Output::new(&self.memory, SOURCE_PREFIX_SPACE)?;
        output.write_stream(batch.batch().schema_ref(), [batch.batch()])?;
        let ops_len = output.len() - SOURCE_PREFIX_SPACE;
        let metadata = self.options.user_metadata.as_ref().map(|metadata| encode_metadata(metadata, &self.memory)).transpose()?;
        let body_size = self.options.default_tz.capacity().checked_add(self.options.user.as_ref().map_or(0, String::capacity)).and_then(|bytes| bytes.checked_add(4096)).ok_or_else(overflow)?;
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
        let (user_metadata, _user_metadata_reservation) = metadata.map(RetainedBytes::into_parts).unzip();
        let tx = proto::Tx {
            tx_ops: None,
            default_tz: Some(self.options.default_tz.clone()), user: self.options.user.clone(),
            user_metadata, external_source_token: None,
            idempotency_guard,
            replay_submission,
            bounded_retry,
            placed_attempt_seed: self.placed_seed.map(|seed| seed.encode().to_vec().into()),
        };
        let tail_len = tx.encoded_len();
        let ops_prefix_len = key_len(TX_OPS_TAG) + encoded_len_varint(ops_len as u64);
        let tx_len = ops_len.checked_add(ops_prefix_len)
            .and_then(|len| len.checked_add(tail_len)).ok_or_else(overflow)?;
        let prefix_len = 1 + key_len(SOURCE_TX_TAG) + encoded_len_varint(tx_len as u64) + ops_prefix_len;
        let start = SOURCE_PREFIX_SPACE - prefix_len;

        output.reserve(tail_len)?;
        let (mut encoded, reservation) = output.into_parts();
        tx.encode(&mut encoded).expect("the source buffer has room for the protobuf tail");

        // Prost writes tx_ops first. Right-align the actual prefix so the IPC stays in place.
        let mut header = &mut encoded[start..SOURCE_PREFIX_SPACE];
        header.put_u8(PROTOBUF_LOG_HEADER);
        encode_key(SOURCE_TX_TAG, WireType::LengthDelimited, &mut header);
        encode_varint(tx_len as u64, &mut header);
        encode_key(TX_OPS_TAG, WireType::LengthDelimited, &mut header);
        encode_varint(ops_len as u64, &mut header);
        RetainedBytes::with_reserved_suffix(encoded, start, reservation)
    }

    fn operation_batch(&self, prefix: Option<&Operation>, purpose: SourcePurpose) -> Result<OwnedBatch> {
        let operations = || prefix.into_iter().chain(&self.operations);
        let scratch = operations().try_fold(65_536usize, |bytes, operation| {
            let extra = match operation {
                Operation::Public(TxOp::OwnedInsertDocs { documents, .. } | TxOp::OwnedReplaceDocs { documents, .. } | TxOp::InsertDocs { documents, .. } | TxOp::PutDocs { documents, .. } | TxOp::PatchDocs { documents, .. }
                    | TxOp::OwnedRelativePeriod { effect: OwnedPeriodEffect::Create(documents) | OwnedPeriodEffect::Replace(documents), .. })
                    | Operation::OwnedWriteIids { documents, .. } | Operation::InsertIids { documents, .. } | Operation::PutIids { documents, .. } | Operation::PatchIids { documents, .. } => documents.rows().iter().try_fold(0usize, |bytes, document| bytes.checked_add(Value::document_retained_bytes(document)?).ok_or_else(overflow))?,
                Operation::Public(TxOp::OwnedArrowDocs { documents, .. }
                    | TxOp::OwnedRelativePeriod { effect: OwnedPeriodEffect::Arrow { documents, .. }, .. })
                | Operation::OwnedArrowIids { documents, .. } => {
                    let names = documents.columns().iter().try_fold(256usize, |bytes, name| {
                        bytes.checked_add(name.len().saturating_mul(2) + 128).ok_or_else(overflow)
                    })?;
                    names.checked_mul(documents.array().len()).ok_or_else(overflow)?
                }
                Operation::Public(TxOp::PutArrowDocs { documents, .. } | TxOp::WriteArrowDocs { documents, .. }) | Operation::InsertArrowIids { documents, .. } | Operation::PutReplayArrow { documents, .. } | Operation::PatchArrowIids { documents, .. } => {
                    documents.array().len().checked_mul(64).ok_or_else(overflow)?
                }
                Operation::Public(TxOp::Sql { sql, args }) => {
                    let rows = args.as_ref().map(SqlArguments::value_bytes).transpose()?.unwrap_or(0);
                    rows.checked_add(sql.capacity()).ok_or_else(overflow)?
                }
                Operation::Public(TxOp::SqlBytes { sql, arg_bytes }) => sql.capacity().checked_add(arg_bytes.as_slice().len()).ok_or_else(overflow)?,
                Operation::Public(TxOp::DeleteDocs { ids, .. } | TxOp::EraseDocs { ids, .. }) => ids.ids().len().checked_mul(128).ok_or_else(overflow)?,
                Operation::Public(TxOp::OwnedDeclareSearchPath { relation, name, column, .. } | TxOp::OwnedDeclareUniquePath { relation, name, column }
                    | TxOp::OwnedDeclareVectorPath { relation, name, column, .. }) =>
                    owned_path_bytes(relation, name, std::slice::from_ref(column))?,
                Operation::Public(TxOp::OwnedCheckCapacity { relation, column, prefix, .. }) =>
                    capacity_condition_bytes(relation, column.capacity(), prefix.capacity())?,
                Operation::Public(TxOp::OwnedDeclareCapacity { relation, column, when, .. }) =>
                    capacity_condition_bytes(relation, column.capacity(), when.capacity())?,
                Operation::Public(TxOp::OwnedDropSpatialPath { relation, name }) =>
                    relation.name().len() + name.name().len(),
                Operation::Public(TxOp::OwnedRetireRelation { relation, .. }) => relation.name().len(),
                Operation::Public(TxOp::OwnedAdoptRelation { source, destination, .. }) =>
                    source.name().len() + destination.name().len(),
                Operation::Public(TxOp::OwnedDeclareRelation { relation, schema }) =>
                    relation.name().len().checked_add(schema.retained_bytes()).ok_or_else(overflow)?,
                Operation::Public(TxOp::OwnedDeclarePath { relation, name, columns } | TxOp::OwnedDeclareSpatialPath { relation, name, columns } | TxOp::OwnedDeclareEncodedSpatialPath { relation, name, columns, .. } | TxOp::OwnedRequireSpatialPath { relation, name, columns, .. }) =>
                    owned_path_bytes(relation, name, columns)?,
                Operation::Public(TxOp::HostedControl(control)) => control.retained_bytes()?,
                Operation::Public(TxOp::OwnedForkWorld { record, attempt, .. }) => Value::document_retained_bytes(record)?.saturating_add(attempt.len() + 128),
                Operation::Public(TxOp::OwnedPublishRoot { attempt, .. } | TxOp::OwnedPublishWorld { attempt, .. }) => attempt.len() + 128,
                Operation::Public(TxOp::OwnedCheckWorldPurge { relation, .. }) => relation.name().len(),
                Operation::Public(TxOp::OwnedDeadline { .. } | TxOp::OwnedClockSlot { .. } | TxOp::OwnedCheckWorldScope { .. } | TxOp::HostedConstraintRefusal) => 0,
                Operation::Public(TxOp::OwnedDeclareFixedRange { relation, rule }) =>
                    relation.name().len() + rule.path.capacity() + rule.column.capacity(),
                Operation::Public(TxOp::OwnedCheckRange { relation, region, .. }) => range_evidence_bytes(relation, region),
                Operation::Public(TxOp::OwnedCheck { relation, id, .. } | TxOp::OwnedDelete { relation, id, .. } | TxOp::OwnedErase { relation, id } | TxOp::OwnedReplayResidue { relation, id, .. } | TxOp::OwnedSweepExpired { relation, id, .. }
                    | TxOp::OwnedRelativePeriod { relation, effect: OwnedPeriodEffect::Delete(id), .. }) =>
                    relation.name().len().checked_add(id.as_value().retained_bytes()?).ok_or_else(overflow)?,
                Operation::Removed(name) => name.capacity(),
            };
            // An operation's own body is a few small fields beside what `extra` counts; a transaction of many small operations is charged for those, not for a fixed allowance each.
            bytes.checked_add(extra.checked_mul(8).and_then(|bytes| bytes.checked_add(OPERATION_BODY_BYTES)).ok_or_else(overflow)?).ok_or_else(overflow)
        })?;
        let _scratch_reservation = self.memory.reserve(scratch)?;
        let mut entries = Vec::new();
        let mut argument_values = None;
        for operation in operations() {
            check_writable_table(operation)?;
            if matches!(operation, Operation::Public(TxOp::DeleteDocs { ids, .. } | TxOp::EraseDocs { ids, .. }) if ids.ids().is_empty()) { continue; }
            entries.push(operation_value(operation, &self.memory, purpose, &mut argument_values)?);
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
        let union = named_union(entries, |leg| operation_leg(leg, &mut arrays, &self.memory, &mut argument_values))?;
        let schema = Arc::new(Schema::new(vec![Field::new("tx-ops", union.data_type().clone(), false)]));
        let batch = RecordBatch::try_new(schema, vec![union]).map_err(|error| invalid(error.to_string()))?;
        arrays.finish(batch)
    }
}

fn landing_ipc_mismatch(field: &Field, array: &dyn Array, masked: &mut bool) -> Option<&'static str> {
    if rtdb_columnar::decimal_field_parameters(field).is_err() { return Some("decimal-metadata"); }
    *masked |= !field.is_nullable() && array.null_count() != 0;
    match field.data_type() {
        DataType::Struct(fields) => fields.iter().zip(array.as_struct().columns())
            .find_map(|(field, column)| landing_ipc_mismatch(field, column.as_ref(), masked)),
        DataType::Union(fields, UnionMode::Dense) => fields.iter()
            .find_map(|(id, field)| landing_ipc_mismatch(field, array.as_union().child(id).as_ref(), masked)),
        DataType::List(item) => landing_ipc_mismatch(item, array.as_list::<i32>().values().as_ref(), masked),
        DataType::LargeList(item) => landing_ipc_mismatch(item, array.as_list::<i64>().values().as_ref(), masked),
        DataType::FixedSizeList(item, _) => landing_ipc_mismatch(item, array.as_fixed_size_list().values().as_ref(), masked),
        DataType::Map(entries, _) => landing_ipc_mismatch(entries, array.as_map().entries(), masked),
        // Arrow IPC omits an empty timezone string from the schema.
        DataType::Timestamp(_, zone) => zone.as_ref().is_some_and(|zone| zone.is_empty()).then_some("empty-timezone"),
        DataType::Null | DataType::Boolean
        | DataType::Int8 | DataType::Int16 | DataType::Int32 | DataType::Int64
        | DataType::UInt8 | DataType::UInt16 | DataType::UInt32 | DataType::UInt64
        | DataType::Float16 | DataType::Float32 | DataType::Float64
        | DataType::Utf8 | DataType::LargeUtf8 | DataType::Binary | DataType::LargeBinary | DataType::FixedSizeBinary(_)
        | DataType::Date32 | DataType::Date64 | DataType::Time32(_) | DataType::Time64(_) | DataType::Duration(_) | DataType::Interval(_)
        | DataType::Decimal32(..) | DataType::Decimal64(..) | DataType::Decimal128(..) | DataType::Decimal256(..) => None,
        DataType::Dictionary(..) => Some("dictionary"),
        DataType::Union(_, UnionMode::Sparse) => Some("sparse-union"),
        DataType::RunEndEncoded(..) => Some("run-end-encoded"),
        DataType::Utf8View | DataType::BinaryView | DataType::ListView(_) | DataType::LargeListView(_) => Some("view"),
        _ => Some("unsupported-ipc-type"),
    }
}

fn decode_operation_pages(
    pages: &[OwnedBatch], legacy: bool, memory: &Arc<MemoryBudget>, parsed_fields: &mut MemoryReservation,
) -> Result<Vec<Operation>> {
    let mut reader = SourceOperationReader { memory, parsed_fields, layouts: BTreeMap::new() };
    let mut operations = Vec::new();
    for page in pages {
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
    Ok(operations)
}

fn decode_placed_seed(tx: &rtdb_format::generated::xtdb::log::proto::Tx) -> Result<Option<PlacedAttemptSeed>> {
    tx.placed_attempt_seed.as_deref().map(PlacedAttemptSeed::decode).transpose()
}

/// The scratch one operation's body takes while it is encoded, beside its documents, arguments or IDs.
const OPERATION_BODY_BYTES: usize = 4_096;
/// The cells one operation adds to the buffers that the encoded batch builds outside its columns.
const OPERATION_CELL_BYTES: usize = 256;

struct SourceOperation<'a> {
    body: SourceOperationBody<'a>,
    iids: Cow<'a, [Iid]>,
    documents: Option<(String, SourceDocuments<'a>)>,
}

enum SourceOperationBody<'a> {
    Values { fields: StructFields<Value>, arguments: Option<i32> },
    Path {
        relation: &'a str,
        name: &'a str,
        columns: Option<&'a [String]>,
        encoding: Option<rtdb_types::SpatialGeometryEncoding>,
    },
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum SourcePurpose { Log, Receipt }

#[derive(Clone, Copy)]
enum SourceDocuments<'a> {
    Rows(&'a Documents),
    Arrow(&'a ArrowDocuments),
    Canonical { documents: &'a ArrowDocuments, omit_nulls: bool },
}

impl SourceDocuments<'_> {
    fn columns(&self) -> &[String] {
        match self {
            Self::Rows(documents) => documents.columns(),
            Self::Arrow(documents) => documents.submitted_rows().map_or_else(|| documents.columns(), Documents::columns),
            Self::Canonical { documents, .. } => documents.columns(),
        }
    }
    fn len(&self) -> usize {
        match self { Self::Rows(documents) => documents.rows().len(), Self::Arrow(documents) | Self::Canonical { documents, .. } => documents.array().len() }
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
    Value::list(documents.rows().iter().map(|row| Value::list(
        row.iter().filter(|(_, value)| !matches!(value, Value::Undefined))
            .map(|(name, _)| Value::Utf8(name.clone())).collect(),
    )).collect())
}

fn encode_owned_arrow_fields(documents: &ArrowDocuments, body: &mut StructFields<Value>) -> Result<Vec<Iid>> {
    let array = documents.array();
    let iids = (0..array.len()).map(|row| documents.owned_id_ref(row).map(rtdb_columnar::EntityIdRef::iid)).collect::<Result<_>>()?;
    body.insert("iids".into(), Value::Null);
    body.insert("field_presence".into(), arrow_field_presence(documents)?);
    body.insert("arrow_payload".into(), Value::Bool(true));
    Ok(iids)
}

fn arrow_field_presence(documents: &ArrowDocuments) -> Result<Value> {
    let array = documents.array();
    let presence = (0..array.len()).map(|row| {
        let mut names = Vec::new();
        for (field, column) in array.fields().iter().zip(array.columns()) {
            let cell = rtdb_columnar::ArrowCell { field, array: column.as_ref(), row };
            if !cell.is_undefined()? { names.push(Value::Utf8(field.name().clone())); }
        }
        Ok(Value::List(names.into()))
    }).collect::<Result<Vec<_>>>()?;
    Ok(Value::List(presence.into()))
}

fn owned_arrow_source<'a>(documents: &'a ArrowDocuments, body: &mut StructFields<Value>) -> Result<(Vec<Iid>, SourceDocuments<'a>)> {
    if documents.legacy_owned_presence().is_none() {
        return Ok((encode_owned_arrow_fields(documents, body)?, SourceDocuments::Arrow(documents)));
    }
    let iids = (0..documents.row_count()).map(|row| documents.owned_id_ref(row).map(rtdb_columnar::EntityIdRef::iid)).collect::<Result<_>>()?;
    legacy_owned_fields(documents, body, true)?;
    Ok((iids, SourceDocuments::Canonical { documents, omit_nulls: true }))
}

fn legacy_owned_fields(documents: &ArrowDocuments, body: &mut StructFields<Value>, encode_implicit_presence: bool) -> Result<()> {
    body.insert("iids".into(), Value::Null);
    if encode_implicit_presence || !matches!(documents.legacy_owned_presence(), Some(LegacyOwnedPresence::Implicit)) {
        let array = documents.array();
        let names = (0..array.len()).map(|row| {
            let mut fields = Vec::new();
            for (field, array) in array.fields().iter().zip(array.columns()) {
                let cell = ArrowCell { field, array: array.as_ref(), row };
                if documents.legacy_owned_cell_is_present(cell)? && !cell.is_undefined()? {
                    fields.push(Value::Utf8(field.name().clone()));
                }
            }
            Ok(Value::list(fields))
        }).collect::<Result<Vec<_>>>()?;
        body.insert("field_presence".into(), Value::list(names));
    }
    Ok(())
}

fn read_owned_field_presence(value: Value) -> Result<Option<Vec<std::collections::BTreeSet<String>>>> {
    let rows = match value {
        Value::Null | Value::Undefined => return Ok(None),
        Value::List(rows) => rows,
        _ => return Err(invalid("Owned field presence is not a row list")),
    };
    let rows = rows.iter().map(|row| {
        let Value::List(fields) = row.as_ref() else { return Err(invalid("Owned field presence is not a field list")); };
        let mut names = std::collections::BTreeSet::new();
        for field in fields.iter() {
            let Value::Utf8(name) = field.as_ref() else { return Err(invalid("Owned field presence name is not text")); };
            if !names.insert(name.clone()) { return Err(invalid("Duplicate owned field presence name")); }
        }
        Ok(names)
    }).collect::<Result<Vec<_>>>()?;
    Ok(Some(rows))
}

fn operation_value<'a>(
    operation: &'a Operation, memory: &Arc<MemoryBudget>, purpose: SourcePurpose, argument_values: &mut Option<crate::ipc::Output>,
) -> Result<(String, SourceOperation<'a>)> {
    let mut body = StructFields::new();
    let mut docs = None;
    let mut arguments = None;
    let mut identities = Cow::Borrowed(&[][..]);
    let tag = match operation {
        Operation::Public(TxOp::Sql { sql, args }) => {
            body.insert("query".into(), Value::Utf8(sql.clone()));
            body.insert("args".into(), Value::Null);
            if let Some(args) = args {
                let values = argument_output(argument_values, memory)?;
                write_arguments(args, memory, purpose, values)?;
                arguments = Some(i32::try_from(values.len()).map_err(|_| overflow())?);
            }
            "sql"
        }
        Operation::Public(TxOp::SqlBytes { sql, arg_bytes }) => {
            body.insert("query".into(), Value::Utf8(sql.clone()));
            body.insert("args".into(), Value::Null);
            if !arg_bytes.as_slice().is_empty() {
                let values = argument_output(argument_values, memory)?;
                values.append_bytes(arg_bytes.as_slice())?;
                arguments = Some(i32::try_from(values.len()).map_err(|_| overflow())?);
            }
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
        Operation::Public(TxOp::OwnedDeclareSearchPath { relation, name, column, analyzer }) => {
            body.insert("relation".into(), Value::Utf8(relation.name().into()));
            body.insert("name".into(), Value::Utf8(name.name().into()));
            body.insert("column".into(), Value::Utf8(column.clone()));
            body.insert("analyzer".into(), Value::Utf8(analyzer.name().into()));
            "rtdb-owned-search-path-v2"
        }
        Operation::Public(TxOp::OwnedDeclareVectorPath { relation, name, column, dimensions, recall }) => {
            body.insert("relation".into(), Value::Utf8(relation.name().into()));
            body.insert("name".into(), Value::Utf8(name.name().into()));
            body.insert("column".into(), Value::Utf8(column.clone()));
            body.insert("dimensions".into(), Value::I64(*dimensions));
            body.insert("recall".into(), Value::F64(*recall));
            "rtdb-owned-vector-path-v1"
        }
        Operation::Public(TxOp::OwnedDropSpatialPath { relation, name }) => {
            return Ok(("rtdb-owned-drop-spatial-path-v1".into(), SourceOperation {
                body: SourceOperationBody::Path { relation: relation.name(), name: name.name(), columns: None, encoding: None },
                iids: identities,
                documents: None,
            }));
        }
        Operation::Public(TxOp::OwnedDeclareRelation { relation, schema }) => {
            body.insert("relation".into(), Value::Utf8(relation.name().into()));
            body.insert("schema".into(), Value::Binary(schema.to_json()?));
            "rtdb-owned-relation-v1"
        }
        Operation::Public(TxOp::OwnedAdoptRelation { source, destination, retain_micros }) => {
            body.insert("source".into(), Value::Utf8(source.name().into()));
            body.insert("destination".into(), Value::Utf8(destination.name().into()));
            body.insert("retain_micros".into(), Value::I64(*retain_micros));
            "rtdb-owned-adopt-relation-v1"
        }
        Operation::Public(TxOp::OwnedRetireRelation { relation, retain_micros }) => {
            body.insert("relation".into(), Value::Utf8(relation.name().into()));
            body.insert("retain_micros".into(), Value::I64(*retain_micros));
            "rtdb-owned-retire-relation-v1"
        }
        Operation::Public(TxOp::OwnedDeclarePath { relation, name, columns } | TxOp::OwnedDeclareSpatialPath { relation, name, columns }) => {
            let tag = if matches!(operation, Operation::Public(TxOp::OwnedDeclareSpatialPath { .. })) {
                "rtdb-owned-spatial-path-v1"
            } else {
                "rtdb-owned-path-v1"
            };
            return Ok((tag.into(), SourceOperation {
                body: SourceOperationBody::Path { relation: relation.name(), name: name.name(), columns: Some(columns), encoding: None },
                iids: identities,
                documents: None,
            }));
        }
        Operation::Public(TxOp::OwnedDeclareEncodedSpatialPath { relation, name, columns, encoding }
            | TxOp::OwnedRequireSpatialPath { relation, name, columns, encoding }) => {
            let tag = if matches!(operation, Operation::Public(TxOp::OwnedRequireSpatialPath { .. })) {
                "rtdb-owned-require-spatial-path-v1"
            } else {
                "rtdb-owned-spatial-path-v2"
            };
            return Ok((tag.into(), SourceOperation {
                body: SourceOperationBody::Path {
                    relation: relation.name(), name: name.name(), columns: Some(columns), encoding: Some(*encoding),
                },
                iids: identities,
                documents: None,
            }));
        }
        Operation::Public(TxOp::OwnedDeclareFixedRange { relation, rule }) => {
            body.insert("relation".into(), Value::Utf8(relation.name().into()));
            body.insert("rule".into(), rule.to_value());
            "rtdb-owned-fixed-range-v1"
        }
        Operation::Public(TxOp::OwnedCheckRange { relation, basis, scope, frontier, until, region }) => {
            body.insert("relation".into(), Value::Utf8(relation.name().into()));
            body.insert("basis_id".into(), Value::I64(basis.tx_id.0));
            body.insert("basis_time".into(), Value::I64(basis.system_time.0));
            body.insert("scope".into(), Value::Binary(scope.encode().to_vec()));
            body.insert("frontier".into(), Value::I64(*frontier));
            body.insert("until".into(), Value::I64(until.0));
            match region.as_deref() {
                Some(rtdb_types::OwnedRangeEvidence::FixedPath(fixed)) => {
                    body.insert("fixed".into(), fixed.to_value());
                    "rtdb-owned-check-range-v2"
                },
                Some(rtdb_types::OwnedRangeEvidence::Entity { anchor, region }) => {
                    body.insert("anchor".into(), Value::I64(*anchor));
                    body.insert("entity".into(), region.id.as_value());
                    body.insert("valid_from".into(), Value::I64(region.valid.lower().0));
                    body.insert("valid_to".into(), Value::I64(region.valid.upper().0));
                    "rtdb-owned-check-range-v3"
                },
                None => "rtdb-owned-check-range-v1",
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
            body = control.source_header();
            if let Some(documents) = control.arrow_replacement() {
                body.insert("iids".into(), Value::Null);
                body.insert("field_presence".into(), arrow_field_presence(documents)?);
                docs = Some((control.name().to_owned(), SourceDocuments::Arrow(documents)));
                "rtdb-hosted-catalog-arrow-v1"
            } else {
                let document = control.source_row().ok_or_else(|| invalid("Hosted control has no source document"))?;
                body.insert("document".into(), Value::Struct(document.clone()));
                if control.is_check() { "rtdb-hosted-control-check-v1" } else { "rtdb-hosted-control-v1" }
            }
        }
        Operation::Public(TxOp::HostedConstraintRefusal) => {
            body.insert("version".into(), Value::I64(1));
            "rtdb-hosted-constraint-refusal-v1"
        }
        Operation::Public(TxOp::OwnedClockSlot { slot }) => {
            body.insert("through".into(), Value::I64(slot.through.0));
            body.insert("reading".into(), Value::I64(slot.reading.0));
            body.insert("issued".into(), Value::Binary(slot.issued.encode().to_vec()));
            body.insert("funded".into(), Value::Bool(slot.funded));
            "rtdb-owned-clock-slot-v4"
        }
        Operation::Public(TxOp::OwnedDeadline { until, recheck_at_staging, journal_before }) => {
            body.insert("until".into(), Value::I64(until.0));
            if let Some(before) = journal_before {
                if !*recheck_at_staging || until.0 <= 0 || before.before() < *until { return Err(invalid("Invalid physical journal deadline")); }
                body.insert("journal_before".into(), Value::I64(before.before().0));
                body.insert("clock_certificate_sha256".into(), Value::Binary(before.certificate_sha256().to_vec()));
                "rtdb-owned-physical-deadline-v2"
            } else if *recheck_at_staging { "rtdb-owned-staging-deadline-v1" } else { "rtdb-owned-deadline-v1" }
        }
        Operation::Public(TxOp::OwnedCheck { relation, id, valid_at, expected }) => {
            body.insert("relation".into(), Value::Utf8(relation.name().into()));
            body.insert("id".into(), id.as_value());
            body.insert("valid_at".into(), Value::I64(valid_at.0));
            body.insert("expected_history".into(), expected.map_or(Value::Null, |version| Value::I64(version.history.0)));
            body.insert("expected_position".into(), expected.map_or(Value::Null, |version| Value::Binary(version.position.to_bytes().to_vec())));
            "rtdb-owned-check-v2"
        }
        Operation::Public(TxOp::OwnedSweepExpired { relation, id, created, expiry, scope }) => {
            body.insert("relation".into(), Value::Utf8(relation.name().into()));
            body.insert("id".into(), id.as_value());
            body.insert("created".into(), Value::I64(created.0));
            body.insert("expiry".into(), Value::I64(expiry.0));
            body.insert("scope".into(), Value::Binary(scope.encode().to_vec()));
            "rtdb-owned-sweep-expired-v2"
        }
        Operation::Public(TxOp::OwnedReplayResidue { relation, id, origin, valid }) => {
            body.insert("relation".into(), Value::Utf8(relation.name().into()));
            body.insert("id".into(), id.as_value());
            body.insert("origin".into(), Value::Binary(origin.0.to_vec()));
            time_fields(&mut body, Some(valid.lower()), Some(valid.upper()));
            "rtdb-owned-replay-residue-v1"
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
        Operation::Public(TxOp::OwnedRelativePeriod { relation, effect, period }) => {
            let (from, to, from_offset, to_offset) = period.fields();
            time_fields(&mut body, from, to);
            body.insert("from_offset_micros".into(), from_offset.map_or(Value::Null, Value::I64));
            body.insert("to_offset_micros".into(), to_offset.map_or(Value::Null, Value::I64));
            match effect {
                OwnedPeriodEffect::Arrow { documents, create_only } => {
                    if documents.legacy_owned_presence().is_none() && documents.row_count() != 1 { return Err(invalid("Relative Arrow writes require one fact")); }
                    let (iids, source) = owned_arrow_source(documents, &mut body)?;
                    identities = Cow::Owned(iids);
                    docs = Some((relation.name().to_owned(), source));
                    if *create_only { "rtdb-owned-relative-insert-v2" } else { "rtdb-owned-relative-replace-v2" }
                },
                OwnedPeriodEffect::Delete(id) => {
                    body.insert("relation".into(), Value::Utf8(relation.name().into()));
                    body.insert("id".into(), id.as_value());
                    "rtdb-owned-relative-delete-v2"
                },
                OwnedPeriodEffect::Create(documents) | OwnedPeriodEffect::Replace(documents) => {
                    identities = Cow::Owned(documents.rows().iter().map(|document| {
                        owned_document_id(document, memory).map(|id| id.iid())
                    }).collect::<Result<_>>()?);
                    body.insert("iids".into(), Value::Null);
                    body.insert("field_presence".into(), owned_field_presence(documents));
                    docs = Some((relation.name().to_owned(), SourceDocuments::Rows(documents)));
                    if matches!(effect, OwnedPeriodEffect::Create(_)) {
                        "rtdb-owned-relative-insert-v2"
                    } else {
                        "rtdb-owned-relative-replace-v2"
                    }
                },
            }
        }
        Operation::Public(TxOp::OwnedArrowDocs { relation, documents, create_only, valid_from, valid_to }) => {
            let (iids, source) = owned_arrow_source(documents, &mut body)?;
            identities = Cow::Owned(iids);
            time_fields(&mut body, *valid_from, *valid_to);
            docs = Some((relation.name().to_owned(), source));
            if *create_only { "rtdb-owned-insert-v1" } else { "rtdb-owned-replace-v1" }
        }
        Operation::OwnedArrowIids { create_only, relation, documents, iids, presence, valid_from, valid_to } => {
            identities = Cow::Borrowed(iids);
            let source = if documents.legacy_owned_presence().is_some() {
                legacy_owned_fields(documents, &mut body, false)?;
                SourceDocuments::Canonical { documents, omit_nulls: true }
            } else {
                let presence = presence.as_ref().ok_or_else(|| invalid("Owned Arrow payload requires exact field presence"))?;
                body.insert("iids".into(), Value::Null);
                body.insert("arrow_payload".into(), Value::Bool(true));
                body.insert("field_presence".into(), Value::list(presence.iter()
                    .map(|fields| Value::list(fields.iter().cloned().map(Value::Utf8).collect())).collect()));
                SourceDocuments::Arrow(documents)
            };
            time_fields(&mut body, *valid_from, *valid_to);
            docs = Some((relation.name().to_owned(), source));
            if *create_only { "rtdb-owned-insert-v1" } else { "rtdb-owned-replace-v1" }
        }
        Operation::Public(TxOp::OwnedInsertDocs { relation, documents, valid_from, valid_to } | TxOp::OwnedReplaceDocs { relation, documents, valid_from, valid_to }) => {
            let iids = documents.rows().iter().map(|document| {
                owned_document_id(document, memory).map(|id| id.iid())
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
        Operation::Public(operation @ (TxOp::PutArrowDocs { table, documents } | TxOp::WriteArrowDocs { table, documents, .. })) => {
            let reader = documents.id_reader()?;
            let iids = (0..documents.array().len()).map(|row| {
                documents.iid(row, reader.as_ref(), memory)
            }).collect::<Result<_>>()?;
            identities = Cow::Owned(iids);
            body.insert("iids".into(), Value::Null);
            docs = Some((table_name(table), SourceDocuments::Arrow(documents)));
            match operation {
                TxOp::WriteArrowDocs { write, valid_from, valid_to, .. } => {
                    time_fields(&mut body, *valid_from, *valid_to);
                    match write { ArrowDocumentWrite::Put => "put-docs", ArrowDocumentWrite::Patch => "patch-docs" }
                }
                _ => {
                    time_fields(&mut body, None, None);
                    "put-docs"
                }
            }
        }
        Operation::InsertArrowIids { table, documents, iids, valid_from, valid_to } => {
            identities = Cow::Borrowed(iids);
            body.insert("iids".into(), Value::Null);
            time_fields(&mut body, *valid_from, *valid_to);
            let documents = if purpose == SourcePurpose::Receipt {
                SourceDocuments::Canonical { documents, omit_nulls: true }
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
                SourceDocuments::Canonical { documents, omit_nulls: true }
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
                SourceDocuments::Canonical { documents, omit_nulls: false }
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
    Ok((tag.into(), SourceOperation { body: SourceOperationBody::Values { fields: body, arguments }, iids: identities, documents: docs }))
}

pub(crate) fn owned_document_id(document: &StructFields<Value>, memory: &Arc<MemoryBudget>) -> Result<EntityId> {
    match document.get("_id") {
        Some(Value::Binary(bytes)) => Ok(EntityId::Binary(bytes.clone())),
        _ => document_id(document, memory),
    }
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

fn operation_leg(
    values: Vec<SourceOperation<'_>>, arrays: &mut BatchBuilder, memory: &Arc<MemoryBudget>,
    argument_values: &mut Option<crate::ipc::Output>,
) -> Result<ArrayRef> {
    if let Some(SourceOperation { body: SourceOperationBody::Path { columns, .. }, .. }) = values.first() {
        let with_columns = columns.is_some();
        return owned_path_leg(values, with_columns, arrays);
    }
    let mut iids = Vec::with_capacity(values.len());
    let mut bodies = Vec::with_capacity(values.len());
    let mut arguments = Vec::with_capacity(values.len());
    let mut documents = Vec::with_capacity(values.len());
    for value in values {
        let SourceOperationBody::Values { fields, arguments: sql_arguments } = value.body else {
            return Err(invalid("Transaction operation leg mixes path and value layouts"));
        };
        iids.push(value.iids);
        bodies.push(fields);
        arguments.push(sql_arguments);
        documents.push(value.documents);
    }
    let values = bodies;
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
        *ty = if arguments.iter().all(Option::is_some) { binary } else { binary.maybe() };
    }
    let mut fields = Vec::new();
    let mut columns = Vec::new();
    // The reference's document writers declare `documents` straight after `iids`, ahead of the valid-time columns.
    let mut documents = documents.iter().any(Option::is_some).then_some(documents);
    for (name, ty) in types {
        let field = Arc::new(to_field(&name, &ty)?);
        let column = if name == "iids" {
            iid_vector(&iids, &field, arrays)?
        } else if name == "args" {
            argument_column(&arguments, argument_values.take(), arrays)?
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

fn owned_path_leg(
    operations: Vec<SourceOperation<'_>>, with_columns: bool, arrays: &mut BatchBuilder,
) -> Result<ArrayRef> {
    use arrow_array::builder::{ListBuilder, StringBuilder};

    let paths = operations.iter().map(|operation| match &operation.body {
        SourceOperationBody::Path { relation, name, columns, encoding } if columns.is_some() == with_columns =>
            Ok((*relation, *name, *columns, *encoding)),
        _ => Err(invalid("Transaction operation leg mixes path layouts")),
    }).collect::<Result<Vec<_>>>()?;
    let mut fields = Vec::new();
    let mut columns = Vec::new();
    let text = LogicalType::Scalar(ScalarType::Utf8);
    for (name, relation) in [("relation", true), ("name", false)] {
        let values = || paths.iter().map(|path| if relation { path.0 } else { path.1 });
        let payload = values().try_fold(0usize, |bytes, value| bytes.checked_add(value.len()).ok_or_else(overflow))?;
        let bytes = payload.checked_add(paths.len() * 8 + 4096).ok_or_else(overflow)?;
        fields.push(Arc::new(to_field(name, &text)?));
        columns.push(arrays.column(bytes, || {
            let mut builder = StringBuilder::with_capacity(paths.len(), payload);
            for value in values() { builder.append_value(value); }
            Ok(Arc::new(builder.finish()))
        })?);
    }
    if with_columns {
        let lists = || paths.iter().flat_map(|path| path.2);
        let names = || lists().flat_map(|list| list.iter());
        let (count, payload) = names().try_fold((0usize, 0usize), |(count, bytes), name| -> Result<_> {
            Ok((count + 1, bytes.checked_add(name.len()).ok_or_else(overflow)?))
        })?;
        let bytes = payload.checked_add((paths.len() + count) * 8 + 4096).ok_or_else(overflow)?;
        let item_type = if count == 0 { LogicalType::Nothing } else { text };
        let field = Arc::new(to_field("columns", &LogicalType::list(item_type))?);
        let DataType::List(item) = field.data_type() else { return Err(invalid("Owned path column names must be a list")); };
        columns.push(arrays.column(bytes, || {
            if item.data_type() == &DataType::Null {
                let offsets = OffsetBuffer::new(vec![0; paths.len() + 1].into());
                return Ok(Arc::new(ListArray::new(Arc::clone(item), offsets, Arc::new(arrow_array::NullArray::new(0)), None)));
            }
            let strings = StringBuilder::with_capacity(count, payload);
            let mut builder = ListBuilder::with_capacity(strings, paths.len()).with_field(Arc::clone(item));
            for list in lists() {
                for name in list {
                    builder.values().append_value(name);
                }
                builder.append(true);
            }
            Ok(Arc::new(builder.finish()))
        })?);
        fields.push(field);
    }
    if paths.first().is_some_and(|path| path.3.is_some()) {
        let values = paths.iter().map(|path| path.3.map(rtdb_types::SpatialGeometryEncoding::name)
            .ok_or_else(|| invalid("Transaction operation leg mixes geometry encodings")))
            .collect::<Result<Vec<_>>>()?;
        let payload = values.iter().try_fold(0usize, |bytes, name| bytes.checked_add(name.len()).ok_or_else(overflow))?;
        let bytes = payload.checked_add(values.len() * 8 + 4096).ok_or_else(overflow)?;
        fields.push(Arc::new(to_field("geometry_encoding", &LogicalType::Scalar(ScalarType::Utf8))?));
        columns.push(arrays.column(bytes, || {
            let mut builder = StringBuilder::with_capacity(values.len(), payload);
            for value in values { builder.append_value(value); }
            Ok(Arc::new(builder.finish()))
        })?);
    }
    rtdb_columnar::build_struct_array(fields.into(), &columns, paths.len(), None)
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
    let offsets = document_offsets(lists.iter().map(SourceDocuments::len))?;
    let length = *offsets.last().expect("document origin") as usize;
    let structs = document_struct(lists, length, arrays, memory)?;
    document_list(structs, offsets)
}

fn document_struct(mut lists: Vec<SourceDocuments<'_>>, length: usize, arrays: &mut BatchBuilder, memory: &Arc<MemoryBudget>) -> Result<ArrayRef> {
    let all_submitted = lists.len() > 1 && lists.iter().all(|list| {
        matches!(list, SourceDocuments::Arrow(documents) if documents.submitted_rows().is_some())
    });
    if !all_submitted {
        if lists.len() > 1 {
            for list in &mut lists {
                if let SourceDocuments::Arrow(documents) = list
                    && let Some(rows) = documents.submitted_rows() {
                    *list = SourceDocuments::Rows(rows);
                }
            }
        }
        if lists.iter().any(|list| matches!(list, SourceDocuments::Arrow(_)))
            && lists.iter().any(|list| matches!(list, SourceDocuments::Canonical { documents, .. } if documents.legacy_owned_presence().is_some()))
        {
            let stable = lists.iter().try_fold(true, |stable, list| match list {
                SourceDocuments::Canonical { documents, .. } if stable => legacy_page_has_stable_layout(documents, arrays),
                _ => Ok(stable),
            })?;
            if stable {
                let pages = lists.iter().map(|list| match list {
                    SourceDocuments::Canonical { documents, .. } => {
                        let mut builder = BatchBuilder::new(4096, memory)?;
                        let array = canonical_document_struct(std::slice::from_ref(list), documents.row_count(), &mut builder, memory)?;
                        own_documents(array, builder, memory).map(Some)
                    }
                    _ => Ok(None),
                }).collect::<Result<Vec<_>>>()?;
                let mixed = lists.iter().zip(&pages).map(|(list, page)| {
                    page.as_ref().map_or(*list, SourceDocuments::Arrow)
                }).collect::<Vec<_>>();
                if let Some(structs) = mixed_document_struct(&mixed, length, arrays)? { return Ok(structs); }
            }
            let legacy = lists.iter().map(|list| match list {
                SourceDocuments::Canonical { documents, .. } if documents.legacy_owned_presence().is_some() => {
                    documents.read_legacy_owned_rows(memory).map(Some)
                }
                _ => Ok(None),
            }).collect::<Result<Vec<_>>>()?;
            let mixed = lists.into_iter().zip(&legacy).map(|(list, rows)| {
                rows.as_ref().map_or(list, SourceDocuments::Rows)
            }).collect();
            return document_struct(mixed, length, arrays, memory);
        }
        let arrow = lists.iter().map(|list| match list {
            SourceDocuments::Arrow(documents) => Some(documents.array()),
            SourceDocuments::Rows(_) | SourceDocuments::Canonical { .. } => None,
        }).collect::<Option<Vec<_>>>();
        if let Some(pages) = arrow
            && let Some(structs) = arrow_document_struct(&pages, length, arrays)? {
            return Ok(structs);
        }
        if lists.iter().all(|list| !matches!(list, SourceDocuments::Canonical { .. }))
            && lists.iter().any(|list| matches!(list, SourceDocuments::Arrow(_)))
            && let Some(structs) = mixed_document_struct(&lists, length, arrays)? {
            return Ok(structs);
        }
    }
    canonical_document_struct(&lists, length, arrays, memory)
}

fn document_observed_type_bytes(cell: ArrowCell<'_>) -> Result<usize> {
    if crate::values::float_vector_field(cell.field) {
        return Ok(cell.field.size().saturating_mul(16).saturating_add(4096));
    }
    cell.observed_type_bytes()
}

fn legacy_page_has_stable_layout(documents: &ArrowDocuments, arrays: &BatchBuilder) -> Result<bool> {
    if documents.legacy_owned_presence().is_none() { return Ok(false); }
    for (field, column) in documents.array().fields().iter().zip(documents.array().columns()) {
        if !crate::values::legacy_owned_arrow_field(field) { return Ok(false); }
        for row in 0..documents.row_count() {
            let cell = ArrowCell { field, array: column.as_ref(), row };
            if !documents.legacy_owned_cell_is_present(cell)? { return Ok(false); }
            let _type = arrays.reserve_scratch(document_observed_type_bytes(cell)?)?;
            if documents.column_type(field.name()) != Some(&cell.observed_type()?) { return Ok(false); }
        }
    }
    Ok(true)
}

fn mixed_document_struct(lists: &[SourceDocuments<'_>], length: usize, arrays: &mut BatchBuilder) -> Result<Option<ArrayRef>> {
    let names = lists.iter().flat_map(SourceDocuments::columns).collect::<OrderedSet<_>>();
    let mut layout = Vec::with_capacity(names.len());
    for name in names {
        let mut ty = LogicalType::Nothing;
        let mut present = 0;
        for list in lists {
            match list {
                SourceDocuments::Arrow(documents) | SourceDocuments::Canonical { documents, .. } => {
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
                    SourceDocuments::Arrow(documents) | SourceDocuments::Canonical { documents, .. } => document_column(documents.array(), name),
                    SourceDocuments::Rows(_) => None,
                };
                (0..list.len()).map(move |row| match list {
                    SourceDocuments::Rows(documents) => documents.rows()[row].get(name).map_or(WriterCell::Null, WriterCell::of),
                    SourceDocuments::Arrow(_) | SourceDocuments::Canonical { .. } => column.map_or(WriterCell::Null, |(field, array)| WriterCell::arrow(field, array, row)),
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
                SourceDocuments::Arrow(documents) | SourceDocuments::Canonical { documents, .. } => document_column_bound(documents.array(), &field, &ty)?,
            };
            bytes.checked_add(size).ok_or_else(overflow)
        })?;
        columns.push(write_document_column(arrays, &field, bound, |selection, scratch| {
            lists.iter().map(|list| match list {
                SourceDocuments::Rows(documents) => {
                    let values = documents.rows().iter().map(|row| struct_field_cell(row, field.name(), &ty).clone()).collect::<Vec<_>>();
                    copy_array(&ty, &values)
                }
                SourceDocuments::Arrow(documents) | SourceDocuments::Canonical { documents, .. } => align_document_column(documents.array(), &field, selection, scratch),
            }).collect()
        })?);
        fields.push(Arc::new(field));
    }
    rtdb_columnar::build_struct_array(fields.into(), &columns, length, None).map(Some)
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

fn arrow_document_struct(pages: &[&StructArray], length: usize, arrays: &mut BatchBuilder) -> Result<Option<ArrayRef>> {
    let Some(first) = pages.first() else { return Ok(None); };
    if pages.len() == 1 {
        let bound = first.get_array_memory_size().checked_add(4096).ok_or_else(overflow)?;
        return arrays.column(bound, || Ok(Arc::new((*first).clone()))).map(Some);
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
    rtdb_columnar::build_struct_array(fields.into(), &columns, length, None).map(Some)
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
    let length = pages.iter().map(ArrowDocuments::row_count).try_fold(0, next_document_offset)? as usize;
    let documents = pages.iter().map(ArrowDocuments::array).collect::<Vec<_>>();
    let array = if let Some(array) = arrow_document_struct(&documents, length, &mut builder)? {
        array
    } else {
        document_struct(pages.iter().map(SourceDocuments::Arrow).collect(), length, &mut builder, memory)?
    };
    own_documents(array, builder, memory)
}

fn own_documents(array: ArrayRef, builder: BatchBuilder, memory: &Arc<MemoryBudget>) -> Result<ArrowDocuments> {
    let batch = builder.finish(RecordBatch::from(array.as_struct().clone()))?;
    let array = StructArray::from(batch.batch().clone());
    let layout = DecodedDocumentLayout::new(array.fields(), memory)?;
    ArrowDocuments::from_source(&array, layout, batch.batch().get_array_memory_size(), memory)?
        .ok_or_else(|| invalid("Document array contains a null row"))
}

pub(crate) enum DocumentCell<'a> {
    Value(Cow<'a, Value>),
    Arrow { cell: ArrowCell<'a>, declared: Option<&'a LogicalType> },
}

impl DocumentCell<'_> {
    pub(crate) fn value(&self) -> Result<Cow<'_, Value>> {
        Ok(match self {
            Self::Value(value) => Cow::Borrowed(value.as_ref()),
            Self::Arrow { declared: Some(LogicalType::Nothing), .. } => Cow::Owned(Value::Undefined),
            Self::Arrow { cell, declared } => {
                let mut value = match declared {
                    Some(logical) => rtdb_columnar::read_value_as(logical, cell.field, cell.array, cell.row)?,
                    None => rtdb_columnar::read_value(cell.field, cell.array, cell.row)?,
                };
                value.admit_object_dates()?;
                Cow::Owned(value)
            }
        })
    }

    fn observed_type(&self, memory: &Arc<MemoryBudget>) -> Result<LogicalType> {
        if let Self::Arrow { cell, declared } = self && float_field(cell.field) {
            let _scratch = memory.reserve(cell.field.size().checked_mul(16).and_then(|bytes| bytes.checked_add(4096)).ok_or_else(overflow)?)?;
            if compatible_float_cell(*cell, *declared) { return cell.observed_type(); }
        }
        if let Self::Arrow { cell, declared: Some(declared) } = self
            && **declared != LogicalType::Nothing
            && canonical::selectable(cell.field)
            && let Ok(scratch) = cell.observed_type_bytes().and_then(|bytes| memory.reserve(bytes))
            && rtdb_columnar::validate_readable_cell(cell.field, cell.array, declared, cell.row, memory).is_ok()
            && let Ok(observed) = cell.observed_type() {
            drop(scratch);
            return Ok(observed);
        }
        let _scratch = memory.reserve(self.bound()?)?;
        Ok(self.value()?.logical_type())
    }

    pub(crate) fn bound(&self) -> Result<usize> {
        match self {
            Self::Value(value) => value.retained_bytes(),
            Self::Arrow { declared: Some(LogicalType::Nothing), .. } => Ok(0),
            Self::Arrow { cell, .. } => rtdb_columnar::estimate_read_cell_bytes(cell.field, cell.array, cell.row),
        }
    }

    fn writer(&self) -> WriterCell<'_> {
        match self {
            Self::Value(value) => WriterCell::of(value),
            Self::Arrow { cell, .. } => WriterCell::arrow(cell.field, cell.array, cell.row),
        }
    }
}

pub(crate) fn owned_projected_documents(
    names: &[String], rows: &[Vec<DocumentCell<'_>>], memory: &Arc<MemoryBudget>,
) -> Result<ArrowDocuments> {
    let mut arrays = BatchBuilder::new(4096, memory)?;
    let _inventory = memory.reserve(canonical_cells_bound(rows.len())?)?;
    let mut schema = Vec::with_capacity(names.len());
    let mut columns = Vec::with_capacity(names.len());
    for (position, name) in names.iter().enumerate() {
        let cells = rows.iter().map(|row| Some(&row[position])).collect::<Vec<_>>();
        let mut ty = LogicalType::Nothing;
        for cell in cells.iter().flatten() {
            let actual = cell.observed_type(memory)?;
            if ty != actual { ty = ty.join(&actual); }
        }
        let (field, column) = canonical_cells(name, ty, &cells, &mut arrays, memory, false)?;
        schema.push(field);
        columns.push(column);
    }
    let array = rtdb_columnar::build_struct_array(schema.into(), &columns, rows.len(), None)?;
    own_documents(array, arrays, memory)
}

fn canonical_cells_bound(rows: usize) -> Result<usize> {
    rows.checked_mul(std::mem::size_of::<Option<DocumentCell<'_>>>() + std::mem::size_of::<WriterCell<'_>>() + std::mem::size_of::<usize>())
        .and_then(|bytes| bytes.checked_add(4096)).ok_or_else(overflow)
}

fn canonical_cells(
    name: &str, mut ty: LogicalType, cells: &[Option<&DocumentCell<'_>>],
    arrays: &mut BatchBuilder, memory: &Arc<MemoryBudget>, legacy_arrow: bool,
) -> Result<(Arc<Field>, ArrayRef)> {
    if struct_backfill_depth(&ty) != 0 {
        let writers = cells.iter().map(|cell| cell.map_or(WriterCell::Null, DocumentCell::writer)).collect::<Vec<_>>();
        ty = backfilled_nullability(ty, &writers);
    }
    let target = to_field(name, &ty)?;
    if legacy_arrow && let Some(column) = canonical_arrow_document_column(&target, cells, arrays)? {
        return Ok((Arc::new(target), column));
    }
    let (array, _writer) = match copy_document_floats(&target, &ty, cells, memory) {
        Ok(Some(output)) => output,
        Ok(None) | Err(_) => {
            if let Some(output) = gather_document_column(&target, &ty, cells, memory) {
                output
            } else {
                let missing = if ty == LogicalType::Nothing { &Value::Undefined } else { &Value::Null };
                rtdb_columnar::copy_evaluated_cells(&ty, cells.len(), memory,
                    |row| cells[row].map_or(Ok(0), DocumentCell::bound),
                    |row| cells[row].map_or_else(|| Ok(missing.clone()), |cell| cell.value().map(Cow::into_owned)),
                )?
            }
        }
    };
    let column = arrays.column(array.get_array_memory_size(), || Ok(array))?;
    Ok((Arc::new(target), column))
}

fn copy_document_floats(
    target: &Field, ty: &LogicalType, cells: &[Option<&DocumentCell<'_>>], memory: &Arc<MemoryBudget>,
) -> Result<Option<(ArrayRef, Arc<MemoryReservation>)>> {
    if !canonical_float_field(target) || !cells.iter().any(|cell| matches!(cell, Some(DocumentCell::Arrow { .. }))) {
        return Ok(None);
    }
    let _inventory = memory.reserve(cells.len().checked_mul(128).and_then(|bytes| bytes.checked_add(4096)).ok_or_else(overflow)?)?;
    if cells.iter().any(|cell| matches!(cell, Some(DocumentCell::Arrow { cell, declared }) if !compatible_float_cell(*cell, *declared))) {
        return Ok(None);
    }
    let values = cells.iter().filter_map(|cell| match cell {
        Some(DocumentCell::Value(value)) => Some(value.as_ref()),
        _ => None,
    }).collect::<Vec<_>>();
    let (values, _rows) = rtdb_columnar::copy_evaluated_cells(ty, values.len(), memory,
        |row| values[row].retained_bytes(), |row| Ok(values[row].clone()))?;
    let mut next_row = 0;
    let picks = cells.iter().map(|cell| match cell {
        None => None,
        Some(DocumentCell::Value(_)) => {
            let row = next_row;
            next_row += 1;
            Some(ArrowCell { field: target, array: values.as_ref(), row })
        }
        Some(DocumentCell::Arrow { cell, .. }) => Some(*cell),
    });
    copy_float_cells(target, picks, memory).map(Some)
}

pub(crate) fn copy_float_cells<'a>(
    target: &Field, cells: impl ExactSizeIterator<Item = Option<ArrowCell<'a>>>, memory: &Arc<MemoryBudget>,
) -> Result<(ArrayRef, Arc<MemoryReservation>)> {
    let base = cells.len().checked_mul(std::mem::size_of::<FloatCell<'_>>()).and_then(|bytes| bytes.checked_add(4096)).ok_or_else(overflow)?;
    let mut charge = memory.reserve(base)?;
    let mut picks = Vec::with_capacity(cells.len());
    for cell in cells {
        let cell_bytes = match cell {
            Some(cell) => rtdb_columnar::estimate_copy_cell_bytes(cell.field, cell.array, cell.row)?,
            None => rtdb_columnar::estimate_null_copy_cell_bytes(target)?,
        };
        let lease = Arc::get_mut(&mut charge).expect("The document column owns its reservation");
        let bound = cell_bytes.checked_mul(4).and_then(|cell| lease.bytes().checked_add(cell)).ok_or_else(overflow)?;
        lease.grow_to(bound)?;
        picks.push(cell.map_or(FloatCell::Null, FloatCell::Arrow));
    }
    let array = gather_float_layout(target, &picks, picks.len() > 128)?;
    drop(picks);
    Arc::get_mut(&mut charge).expect("The document column owns its reservation").shrink_to(array.get_array_memory_size())?;
    Ok((array, charge))
}

#[derive(Clone, Copy)]
enum FloatCell<'a> {
    Arrow(ArrowCell<'a>),
    Null,
    Masked,
}

fn gather_float_layout(target: &Field, cells: &[FloatCell<'_>], interleaved: bool) -> Result<ArrayRef> {
    use arrow_array::types::{Float32Type, Float64Type};

    fn visible(cell: FloatCell<'_>) -> Result<Option<ArrowCell<'_>>> {
        let FloatCell::Arrow(cell) = cell else { return Ok(None); };
        let cell = cell.physical()?;
        Ok((!cell.is_null()?).then_some(cell))
    }
    let mismatch = || invalid("Floating-point document cells disagree with their canonical layout");
    macro_rules! primitive {
        ($ty:ty) => {{
            let values = cells.iter().map(|&cell| visible(cell)?.map(|cell| {
                if cell.field.data_type() != target.data_type() { return Err(mismatch()); }
                Ok(cell.array.as_primitive::<$ty>().value(cell.row))
            }).transpose()).collect::<Result<arrow_array::PrimitiveArray<$ty>>>()?;
            Ok(Arc::new(values))
        }};
    }
    match target.data_type() {
        DataType::Float32 => primitive!(Float32Type),
        DataType::Float64 => primitive!(Float64Type),
        DataType::Null => {
            for &cell in cells {
                if visible(cell)?.is_some() { return Err(mismatch()); }
            }
            Ok(Arc::new(arrow_array::NullArray::new(cells.len())))
        }
        DataType::Union(legs, UnionMode::Dense) => {
            let mut groups = vec![Vec::new(); legs.len()];
            let mut tags = Vec::with_capacity(cells.len());
            let mut offsets = Vec::with_capacity(cells.len());
            for &input in cells {
                let cell = visible(input)?;
                let (index, (id, _)) = if matches!(input, FloatCell::Masked) {
                    // The row writer pads under the first leg within a chunk. Interleaving row-writer chunks uses the null leg.
                    let mut legs = legs.iter().enumerate();
                    let first = legs.next();
                    first.filter(|(_, (_, field))| !interleaved || field.data_type() == &DataType::Null)
                        .or_else(|| legs.find(|(_, (_, field))| field.data_type() == &DataType::Null))
                        .or(first)
                } else {
                    legs.iter().enumerate().find(|(_, (_, leg))| {
                        match (cell.map(|cell| cell.field.data_type()), leg.data_type()) {
                            (None, DataType::Null) => true,
                            (Some(DataType::List(_)), DataType::List(_)) => true,
                            (Some(DataType::FixedSizeList(_, a)), DataType::FixedSizeList(_, b)) => a == b,
                            (Some(source), target) => source == target,
                            _ => false,
                        }
                    })
                }.ok_or_else(mismatch)?;
                tags.push(id);
                offsets.push(i32::try_from(groups[index].len()).map_err(|_| overflow())?);
                groups[index].push(cell.map_or(input, FloatCell::Arrow));
            }
            let children = legs.iter().zip(&groups).map(|((_, field), cells)| gather_float_layout(field, cells, interleaved)).collect::<Result<Vec<_>>>()?;
            Ok(Arc::new(UnionArray::try_new(legs.clone(), tags.into(), Some(offsets.into()), children)
                .map_err(|error| invalid(error.to_string()))?))
        }
        DataType::List(item) | DataType::FixedSizeList(item, _) => {
            let width = match target.data_type() {
                DataType::FixedSizeList(_, width) => Some(usize::try_from(*width).map_err(|_| mismatch())?),
                _ => None,
            };
            let mut children = Vec::new();
            let mut offsets = vec![0i32];
            let mut valid = Vec::with_capacity(cells.len());
            for &cell in cells {
                let cell = visible(cell)?;
                valid.push(cell.is_some());
                if let Some(cell) = cell {
                    let actual_width = match cell.field.data_type() {
                        DataType::List(_) => None,
                        DataType::FixedSizeList(_, width) => Some(usize::try_from(*width).map_err(|_| mismatch())?),
                        _ => return Err(mismatch()),
                    };
                    if width != actual_width { return Err(mismatch()); }
                    let (field, array, range) = cell.sequence_range().ok_or_else(mismatch)?;
                    children.extend(range.map(|row| FloatCell::Arrow(ArrowCell { field, array, row })));
                } else if let Some(width) = width {
                    children.extend(std::iter::repeat_n(FloatCell::Masked, width));
                }
                if width.is_none() {
                    offsets.push(i32::try_from(children.len()).map_err(|_| overflow())?);
                }
            }
            let values = gather_float_layout(item, &children, interleaved)?;
            let layout = arrow_array::new_empty_array(target.data_type()).to_data();
            let nulls = valid.iter().any(|valid| !valid).then(|| NullBuffer::from(valid));
            let mut data = layout.into_builder().len(cells.len()).nulls(nulls).child_data(vec![values.to_data()]);
            if width.is_none() { data = data.buffers(vec![ScalarBuffer::from(offsets).into_inner()]); }
            Ok(arrow_array::make_array(data.build().map_err(|error| invalid(error.to_string()))?))
        }
        _ => Err(mismatch()),
    }
}

fn canonical_document_struct(
    lists: &[SourceDocuments<'_>], length: usize, arrays: &mut BatchBuilder, memory: &Arc<MemoryBudget>,
) -> Result<ArrayRef> {
    let legacy_arrow = lists.iter().any(|list| match list {
        SourceDocuments::Rows(_) => false,
        SourceDocuments::Arrow(documents) | SourceDocuments::Canonical { documents, .. } => documents.legacy_owned_presence().is_some(),
    });
    let names = lists.iter().flat_map(SourceDocuments::columns).collect::<OrderedSet<_>>();
    let _inventory = memory.reserve(canonical_cells_bound(length)?)?;
    let mut columns = Vec::with_capacity(names.len());
    let mut schema = Vec::with_capacity(names.len());
    for name in names {
        let mut ty = LogicalType::Nothing;
        let mut present = 0usize;
        let mut cells = Vec::with_capacity(length);
        for list in lists {
            let start = cells.len();
            let submitted = match list {
                SourceDocuments::Rows(documents) => Some(*documents),
                SourceDocuments::Arrow(documents) => documents.submitted_rows(),
                SourceDocuments::Canonical { .. } => None,
            };
            match (submitted, list) {
                (Some(documents), _) => {
                    if let Some(declared) = documents.declared_types().and_then(|columns| columns.get(name))
                        && ty != *declared {
                        ty = ty.join(declared);
                    }
                    cells.extend(documents.rows().iter().map(|row| row.get(name).map(|value| DocumentCell::Value(Cow::Borrowed(value)))));
                }
                (None, SourceDocuments::Arrow(documents) | SourceDocuments::Canonical { documents, .. }) => {
                    if let Some((field, array)) = document_column(documents.array(), name) {
                        let declared = documents.column_type(name).ok_or_else(|| invalid("Document column has no logical type"))?;
                        if !matches!(documents.legacy_owned_presence(), Some(LegacyOwnedPresence::Submitted)) && &ty != declared {
                            ty = ty.join(declared);
                        }
                        let preserve_null = documents.legacy_owned_presence().is_some()
                            || !matches!(list, SourceDocuments::Canonical { omit_nulls: true, .. }) || name == "_id";
                        for row in 0..array.len() {
                            if documents.legacy_owned_presence().is_some() && !documents.legacy_owned_cell_is_present(ArrowCell { field, array: array.as_ref(), row })? {
                                cells.push(None);
                                continue;
                            }
                            if !preserve_null && !matches!(declared, LogicalType::Nothing)
                                && matches!(WriterCell::arrow(field, array.as_ref(), row), WriterCell::Null) {
                                cells.push(None);
                                continue;
                            }
                            cells.push(Some(DocumentCell::Arrow { cell: ArrowCell { field, array: array.as_ref(), row }, declared: Some(declared) }));
                        }
                    } else {
                        cells.extend(std::iter::repeat_with(|| None).take(list.len()));
                    }
                }
                (None, SourceDocuments::Rows(_)) => unreachable!("row documents retain their submitted layout"),
            }
            for cell in cells[start..].iter().flatten() {
                let actual = if legacy_arrow
                    && let DocumentCell::Arrow { cell, .. } = cell
                    && !float_field(cell.field)
                    && crate::values::legacy_owned_arrow_field(cell.field)
                    && crate::values::valid_argument_union_cell(cell.field, cell.array, cell.row)
                {
                    let _scratch = memory.reserve(cell.observed_type_bytes()?)?;
                    cell.observed_type()?
                } else {
                    cell.observed_type(memory)?
                };
                if ty != actual { ty = ty.join(&actual); }
                present = present.checked_add(1).ok_or_else(overflow)?;
            }
        }
        if present < length || present == 0 { ty = ty.maybe(); }
        if struct_backfill_depth(&ty) == 0
            && let Ok(field) = to_field(name, &ty)
            && let Ok(Some((array, _writer))) = coalesce_submitted_numeric_lists(lists, &field, memory)
            && let Ok(column) = arrays.column(array.get_array_memory_size(), || Ok(array)) {
            columns.push(column);
            schema.push(Arc::new(field));
            continue;
        }
        let cells = cells.iter().map(Option::as_ref).collect::<Vec<_>>();
        let (field, column) = canonical_cells(name, ty, &cells, arrays, memory, legacy_arrow)?;
        schema.push(field);
        columns.push(column);
    }
    rtdb_columnar::build_struct_array(schema.into(), &columns, length, None).map_err(|error| invalid(error.to_string()))
}

fn gather_document_column(
    field: &Field, ty: &LogicalType, cells: &[Option<&DocumentCell<'_>>], memory: &Arc<MemoryBudget>,
) -> Option<(ArrayRef, Arc<MemoryReservation>)> {
    use rtdb_columnar::{copy_nullable_cells, estimate_copy_cell_bytes, estimate_null_copy_cell_bytes};

    if ty.unencoded() != *ty || !canonical::selectable(field)
        || !cells.iter().flatten().any(|cell| matches!(cell, DocumentCell::Arrow { .. })) {
        return None;
    }
    let _inventory = memory.reserve(cells.len().checked_mul(128)?.checked_add(4096)?).ok()?;
    let mut sources = Vec::new();
    let mut picks = Vec::with_capacity(cells.len());
    let mut previous = None;
    // copy_evaluated_cells clears packed null payloads when it joins multiple chunks.
    let packed_null_bytes = cells.len() <= 128;
    let row_values = || cells.iter().flatten().filter_map(|cell| match cell { DocumentCell::Value(value) => Some(value.as_ref()), _ => None });
    if row_values().next().is_some() {
        let reserve = memory.reserve(estimate_builder_bytes_refs(ty, row_values()).ok()?).ok()?;
        let array = rtdb_columnar::copy_array_refs(ty, &row_values().collect::<Vec<_>>()).ok()?;
        if !(0..array.len()).all(|row| canonical::selectable_cell(ArrowCell { field, array: array.as_ref(), row }, packed_null_bytes)) {
            return None;
        }
        sources.push((array, Some(reserve)));
    }
    let mut next_value = 0;
    for cell in cells {
        match cell {
            None => picks.push(None),
            Some(DocumentCell::Value(_)) => {
                picks.push(Some((0, next_value)));
                next_value += 1;
            }
            Some(DocumentCell::Arrow { cell: selected, .. }) => {
                let ArrowCell { field: source, array: column, row } = *selected;
                if !canonical::selectable(source) || !canonical::selectable_cell(*selected, packed_null_bytes) {
                    return None;
                }
                if previous.is_none_or(|prior| !std::ptr::eq(prior, column)) {
                    sources.push(canonical::align(&arrow_array::make_array(column.to_data()), source, field, memory)?);
                    previous = Some(column);
                }
                picks.push(Some((sources.len() - 1, row)));
            }
        }
    }
    let inputs = sources.iter().map(|(array, _)| array.as_ref()).collect::<Vec<_>>();
    let null = estimate_null_copy_cell_bytes(field).ok()?;
    let bound = picks.iter().try_fold(4096usize, |size, pick| {
        let cell = match pick {
            Some((page, row)) => estimate_copy_cell_bytes(field, inputs[*page], *row).ok()?,
            None => null,
        };
        size.checked_add(cell.checked_mul(4)?)
    })?;
    let mut scratch = memory.reserve(bound).ok()?;
    let array = copy_nullable_cells(&inputs, &picks).ok()?;
    Arc::get_mut(&mut scratch).expect("The document gather owns its output")
        .shrink_to(array.get_array_memory_size()).ok()?;
    Some((array, scratch))
}

fn masks_dense_union(field: &Field, parent_can_mask: bool) -> bool {
    match field.data_type() {
        DataType::Struct(children) => children.iter().any(|child| masks_dense_union(child, parent_can_mask || field.is_nullable())),
        DataType::FixedSizeList(child, _) => masks_dense_union(child, parent_can_mask || field.is_nullable()),
        DataType::List(child) => masks_dense_union(child, false),
        DataType::Union(legs, UnionMode::Dense) => parent_can_mask || legs.iter().any(|(_, child)| masks_dense_union(child, false)),
        _ => false,
    }
}

fn canonical_arrow_document_column(field: &Field, cells: &[Option<&DocumentCell<'_>>], arrays: &mut BatchBuilder) -> Result<Option<ArrayRef>> {
    if canonical_float_field(field) || !crate::values::legacy_owned_arrow_field(field) || cells.is_empty() { return Ok(None); }
    if masks_dense_union(field, false) {
        // The legacy writer's chunk concatenation can retag union cells masked by a null parent.
        return Ok(None);
    }
    let _selection = arrays.reserve_scratch(cells.len().checked_mul(128).and_then(|bytes| bytes.checked_add(4096)).ok_or_else(overflow)?)?;
    let mut sources: Vec<(&Field, &dyn Array)> = Vec::new();
    let mut picks = Vec::with_capacity(cells.len());
    let mut bound = cells.len().checked_mul(64).and_then(|bytes| bytes.checked_add(field.size().checked_mul(8)?))
        .and_then(|bytes| bytes.checked_add(4096)).ok_or_else(overflow)?;
    for cell in cells {
        let Some(DocumentCell::Arrow { cell: selected, .. }) = cell else {
            if cell.is_some() { return Ok(None); }
            picks.push(None);
            continue;
        };
        let ArrowCell { field: source, array: column, row } = *selected;
        if !crate::values::legacy_owned_arrow_field(source) { return Ok(None); }
        if selected.is_null()? {
            picks.push(None);
        } else {
            if !crate::values::valid_argument_union_cell(source, column, row) { return Ok(None); }
            let position = if sources.last().is_some_and(|(last_field, array)| std::ptr::eq(*last_field, source) && std::ptr::eq(*array, column)) {
                sources.len() - 1
            } else {
                sources.push((source, column));
                sources.len() - 1
            };
            picks.push(Some((position, row)));
        }
        bound = bound.checked_add(rtdb_columnar::estimate_event_copy_cell_bytes(source, column, row)?.checked_mul(4).ok_or_else(overflow)?).ok_or_else(overflow)?;
    }
    if sources.is_empty() { return Ok(None); }
    let mut inputs = Vec::with_capacity(sources.len());
    let mut scratch = Vec::new();
    for (source, column) in sources {
        if source.data_type() == field.data_type() {
            inputs.push(arrow_array::make_array(column.to_data()));
        } else {
            let Ok(bytes) = rtdb_columnar::arrow_relayout_bound(column, source, field) else { return Ok(None); };
            let lease = arrays.reserve_scratch(bytes)?;
            let Ok((_, aligned)) = rtdb_columnar::relayout_arrow_column(arrow_array::make_array(column.to_data()), source, field) else { return Ok(None); };
            if aligned.data_type() != field.data_type() { return Ok(None); }
            inputs.push(aligned);
            scratch.push(lease);
        }
    }
    let columns = inputs.iter().map(|array| array.as_ref()).collect::<Vec<_>>();
    arrays.column(bound, || rtdb_columnar::copy_event_cells(&columns, &picks)).map(Some)
}

fn legacy_owned_arrow_documents(
    array: &StructArray, layout: &Arc<DecodedDocumentLayout>, source_bytes: usize,
    presence: Option<&[std::collections::BTreeSet<String>]>, memory: &Arc<MemoryBudget>,
) -> Result<Option<ArrowDocuments>> {
    use crate::values::{legacy_owned_arrow_field, legacy_owned_field_name, valid_argument_union_cell};

    if array.null_count() != 0 || array.is_empty()
        || presence.is_some_and(|fields| fields.len() != array.len())
        || !array.fields().iter().all(|field| {
            (legacy_owned_field_name(field.name()) || field.name() == rtdb_types::WorldShadow::FIELDS)
                && legacy_owned_arrow_field(field)
        }) {
        return Ok(None);
    }
    let mut fields = Vec::with_capacity(array.num_columns());
    let mut columns = Vec::with_capacity(array.num_columns());
    let mut aligned = false;
    for (field, column) in array.fields().iter().zip(array.columns()) {
        let logical = layout.column_type(field.name()).ok_or_else(|| invalid("Document column has no logical type"))?;
        if (0..array.len()).any(|row| !valid_argument_union_cell(field, column.as_ref(), row))
        {
            return Ok(None);
        }
        if *logical != LogicalType::Nothing
            && rtdb_columnar::validate_readable_column(field, column.as_ref(), logical, memory).is_err()
        {
            return Ok(None);
        }
        let target = to_field(field.name(), logical)?;
        if field.as_ref() == &target {
            fields.push(Arc::clone(field));
            columns.push(Arc::clone(column));
        } else {
            let Ok(bound) = rtdb_columnar::arrow_relayout_bound(column.as_ref(), field, &target) else { return Ok(None); };
            let reservation = memory.reserve(bound)?;
            let Ok((_, column)) = rtdb_columnar::relayout_arrow_column(Arc::clone(column), field, &target) else { return Ok(None); };
            if column.data_type() != target.data_type() { return Ok(None); }
            fields.push(Arc::new(target));
            columns.push(rtdb_columnar::retain_array(column, &reservation)?);
            aligned = true;
        }
    }
    if !aligned { return ArrowDocuments::from_source(array, Arc::clone(layout), source_bytes, memory); }
    let fields = fields.into();
    let layout = DecodedDocumentLayout::new(&fields, memory)?;
    let canonical = rtdb_columnar::build_struct_array(fields, &columns, array.len(), None)?;
    ArrowDocuments::from_source(canonical.as_struct(), layout, source_bytes, memory)
}

fn coalesce_submitted_numeric_lists(
    lists: &[SourceDocuments<'_>], field: &Field, memory: &Arc<MemoryBudget>,
) -> Result<Option<(ArrayRef, Arc<MemoryReservation>)>> {
    let DataType::List(element) = field.data_type() else { return Ok(None); };
    if lists.len() < 2 || !field.metadata().is_empty()
        || !rtdb_types::ListValue::supports_numeric_arrow(element) {
        return Ok(None);
    }
    fn submitted_page<'a>(list: &SourceDocuments<'a>) -> Option<&'a StructArray> {
        match *list {
            SourceDocuments::Arrow(documents) if documents.submitted_rows().is_some() => Some(documents.array()),
            _ => None,
        }
    }
    let Some(pages) = lists.iter().map(submitted_page).collect::<Option<Vec<_>>>() else { return Ok(None); };
    let ty = from_field(field)?;
    let bound = pages.iter().try_fold(4096usize, |bytes, page| {
        bytes.checked_add(document_column_bound(page, field, &ty)?).ok_or_else(overflow)
    })?;
    let mut reservation = memory.reserve(bound)?;
    let columns = pages.iter().map(|page| align_document_column(page, field, None, &mut Vec::new()))
        .collect::<Result<Vec<_>>>()?;
    let counts = columns.iter().try_fold((0usize, 0i32), |(rows, children), column| {
        if column.data_type() != field.data_type() { return None; }
        let offsets = column.as_list::<i32>().value_offsets();
        Some((rows.checked_add(column.len())?, children.checked_add(offsets.last()? - offsets.first()?)?))
    });
    let Some((rows, children)) = counts else { return Ok(None); };
    let selection_bound = rows.checked_add(children as usize).and_then(|cells| cells.checked_mul(128)).ok_or_else(overflow)?;
    let selection = memory.reserve(selection_bound)?;
    let inputs = columns.iter().map(|column| column.as_ref()).collect::<Vec<_>>();
    let picks = inputs.iter().enumerate().flat_map(|(page, column)| (0..column.len()).map(move |row| (page, row))).collect::<Vec<_>>();
    let array = rtdb_columnar::copy_selected_cells(&inputs, &picks)?;
    drop(picks);
    drop(inputs);
    drop(columns);
    drop(selection);
    Arc::get_mut(&mut reservation).expect("Coalescing owns the column reservation").shrink_to(array.get_array_memory_size())?;
    Ok(Some((array, reservation)))
}

fn document_offsets(lengths: impl IntoIterator<Item = usize>) -> Result<Vec<i32>> {
    let mut offsets = vec![0i32];
    for length in lengths {
        offsets.push(next_document_offset(*offsets.last().unwrap(), length)?);
    }
    Ok(offsets)
}

fn next_document_offset(offset: i32, length: usize) -> Result<i32> {
    let length = i32::try_from(length).map_err(|_| overflow())?;
    offset.checked_add(length).ok_or_else(overflow)
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
            "rtdb-owned-vector-path-v1" => &["relation", "name", "column", "dimensions", "recall"],
            "rtdb-owned-drop-spatial-path-v1" => &["relation", "name"],
            "rtdb-owned-relation-v1" => &["relation", "schema"],
            "rtdb-owned-retire-relation-v1" => &["relation", "retain_micros"],
            "rtdb-owned-capacity-v1" => &["relation", "column", "prefix_bytes", "cap"],
            "rtdb-owned-check-capacity-v1" => &["relation", "column", "prefix", "at_most"],
            "rtdb-owned-path-v1" | "rtdb-owned-spatial-path-v1" => &["relation", "name", "columns"],
            "rtdb-owned-spatial-path-v2" | "rtdb-owned-require-spatial-path-v1" => &["relation", "name", "columns", "geometry_encoding"],
            "rtdb-hosted-control-v1" | "rtdb-hosted-control-check-v1" => &["relation", "scope", "expected", "document"],
            "rtdb-hosted-catalog-arrow-v1" => &["relation", "scope", "expected", "field_presence"],
            "rtdb-owned-clock-slot-v1" => &["predecessor_id", "predecessor_micros"],
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
            "rtdb-owned-check-range-v3" => &["relation", "basis_id", "basis_time", "scope", "frontier", "until", "anchor", "entity", "valid_from", "valid_to"],
            "rtdb-owned-check-v2" => &["relation", "id", "valid_at", "expected_history", "expected_position"],
            "rtdb-owned-erase-v2" => &["relation", "id"],
            "rtdb-owned-sweep-expired-v2" => &["relation", "id", "created", "expiry", "scope"],
            "rtdb-owned-delete-v1" => &["relation", "id", "_valid_from", "_valid_to"],
            "rtdb-owned-replay-residue-v1" => &["relation", "id", "origin", "_valid_from", "_valid_to"],
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
                TxOp::OwnedCheckCapacity { relation: rtdb_types::OwnedRelation::from_source(relation)?, column, prefix, at_most }
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
            "rtdb-owned-search-path-v2" => {
                let (Value::Utf8(relation), Value::Utf8(name), Value::Utf8(column), Value::Utf8(analyzer)) =
                    (value("relation")?, value("name")?, value("column")?, value("analyzer")?)
                else { return Err(invalid("Owned search path declaration has invalid fields")); };
                let analyzer = rtdb_plan::parse_analyzer(&analyzer)?;
                TxOp::OwnedDeclareSearchPath { relation: rtdb_types::OwnedRelation::new(relation)?, name: rtdb_types::OwnedRelation::new(name)?, column, analyzer }
            }
            "rtdb-owned-vector-path-v1" => {
                let (Value::Utf8(relation), Value::Utf8(name), Value::Utf8(column), Value::I64(dimensions), Value::F64(recall)) =
                    (value("relation")?, value("name")?, value("column")?, value("dimensions")?, value("recall")?)
                else { return Err(invalid("Owned vector path declaration has invalid fields")); };
                TxOp::OwnedDeclareVectorPath {
                    relation: rtdb_types::OwnedRelation::new(relation)?, name: rtdb_types::OwnedRelation::new(name)?, column, dimensions, recall,
                }
            }
            "rtdb-owned-relation-v1" => {
                let (Value::Utf8(relation), Value::Binary(schema)) = (value("relation")?, value("schema")?)
                    else { return Err(invalid("Owned relation declaration has invalid fields")); };
                TxOp::OwnedDeclareRelation {
                    relation: rtdb_types::OwnedRelation::new(relation)?,
                    schema: rtdb_types::OwnedSchema::from_json(&schema)?,
                }
            }
            "rtdb-owned-adopt-relation-v1" => {
                let (Value::Utf8(source), Value::Utf8(destination), Value::I64(retain_micros)) =
                    (value("source")?, value("destination")?, value("retain_micros")?)
                else { return Err(invalid("Owned relation adoption has invalid fields")); };
                TxOp::OwnedAdoptRelation {
                    source: rtdb_types::OwnedRelation::new(source)?,
                    destination: rtdb_types::OwnedRelation::new(destination)?,
                    retain_micros,
                }
            }
            "rtdb-owned-retire-relation-v1" => {
                let (Value::Utf8(relation), Value::I64(retain_micros)) = (value("relation")?, value("retain_micros")?)
                    else { return Err(invalid("Owned relation retirement has invalid fields")); };
                rtdb_types::OwnedRetirement::at(Timestamp(0), retain_micros)?;
                TxOp::OwnedRetireRelation { relation: rtdb_types::OwnedRelation::new(relation)?, retain_micros }
            }
            "rtdb-owned-path-v1" | "rtdb-owned-spatial-path-v1" | "rtdb-owned-spatial-path-v2" | "rtdb-owned-require-spatial-path-v1" | "rtdb-owned-drop-spatial-path-v1" => {
                let cell = |key| {
                    let index = body.fields().iter().position(|field| field.name() == key)?;
                    Some(ArrowCell {
                        field: &body.fields()[index],
                        array: body.column(index).as_ref(),
                        row: offset,
                    })
                };
                let text = |key| cell(key).and_then(ArrowCell::utf8)
                    .ok_or_else(|| invalid("Owned path requires text relation and name"));
                let relation = rtdb_types::OwnedRelation::new(text("relation")?)?;
                let path = rtdb_types::OwnedRelation::new(text("name")?)?;
                if name == "rtdb-owned-drop-spatial-path-v1" {
                    TxOp::OwnedDropSpatialPath { relation, name: path }
                } else {
                    let columns = cell("columns").and_then(ArrowCell::list)
                        .ok_or_else(|| invalid("Owned path requires a text column list"))?;
                    let columns = columns.map(|column| column.utf8().map(str::to_owned)
                        .ok_or_else(|| invalid("Owned path names a non-text column")))
                        .collect::<Result<Vec<_>>>()?;
                    match name.as_str() {
                        "rtdb-owned-spatial-path-v1" => TxOp::OwnedDeclareSpatialPath { relation, name: path, columns },
                        "rtdb-owned-spatial-path-v2" | "rtdb-owned-require-spatial-path-v1" => {
                            let encoding = cell("geometry_encoding").and_then(ArrowCell::utf8)
                                .ok_or_else(|| invalid("Owned spatial path requires a geometry encoding"))?;
                            let encoding = rtdb_types::SpatialGeometryEncoding::parse(encoding)?;
                            if name == "rtdb-owned-require-spatial-path-v1" {
                                TxOp::OwnedRequireSpatialPath { relation, name: path, columns, encoding }
                            } else {
                                TxOp::OwnedDeclareEncodedSpatialPath { relation, name: path, columns, encoding }
                            }
                        }
                        _ => TxOp::OwnedDeclarePath { relation, name: path, columns },
                    }
                }
            }
            "rtdb-owned-fixed-range-v1" => {
                let Value::Utf8(relation) = value("relation")? else { return Err(invalid("Fixed range declaration has no relation")); };
                TxOp::OwnedDeclareFixedRange {
                    relation: rtdb_types::OwnedRelation::new(relation)?,
                    rule: rtdb_types::FixedRangeRule::from_value(&value("rule")?)?,
                }
            }
            "rtdb-owned-check-range-v1" | "rtdb-owned-check-range-v2" | "rtdb-owned-check-range-v3" => {
                let (Value::Utf8(relation), Value::I64(basis_id), Value::I64(basis_time),
                    Value::Binary(scope), Value::I64(frontier), Value::I64(until)) =
                    (value("relation")?, value("basis_id")?, value("basis_time")?, value("scope")?, value("frontier")?, value("until")?)
                else { return Err(invalid("Owned range witness has invalid fields")); };
                TxOp::OwnedCheckRange {
                    relation: rtdb_types::OwnedRelation::new(relation)?,
                    basis: rtdb_types::TransactionKey { tx_id: rtdb_types::TxId(basis_id), system_time: Timestamp(basis_time) },
                    scope: rtdb_types::PlacedAttemptScope::decode(&scope)?,
                    frontier, until: Timestamp(until),
                    region: match name.as_str() {
                        "rtdb-owned-check-range-v2" => Some(Arc::new(rtdb_types::OwnedRangeEvidence::FixedPath(
                            rtdb_types::FixedRangeEvidence::from_value(&value("fixed")?)?,
                        ))),
                        "rtdb-owned-check-range-v3" => {
                            let (Value::I64(anchor), Value::I64(from), Value::I64(to)) =
                                (value("anchor")?, value("valid_from")?, value("valid_to")?)
                            else { return Err(invalid("Owned entity range witness has invalid bounds")); };
                            let valid = rtdb_types::Interval::from_micros(from, to)?;
                            if anchor < 0 || valid.is_empty() {
                                return Err(invalid("Owned entity range witness needs a covered, nonempty region"));
                            }
                            let id = value("entity")?.owned_entity_id()?;
                            Some(Arc::new(rtdb_types::OwnedRangeEvidence::Entity {
                                anchor, region: rtdb_types::OwnedEntityRegion { id, valid },
                            }))
                        },
                        _ => None,
                    },
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
            "rtdb-hosted-control-v1" | "rtdb-hosted-control-check-v1" | "rtdb-hosted-catalog-arrow-v1" => {
                let (Value::Utf8(relation), Value::Binary(scope), expected) =
                    (value("relation")?, value("scope")?, value("expected")?)
                else { return Err(invalid("Hosted control has invalid fields")); };
                let expected = match expected {
                    Value::I64(time) => Some(Timestamp(time)),
                    Value::Null => None,
                    _ => return Err(invalid("Hosted control has an invalid expectation")),
                };
                let relation = rtdb_types::OwnedRelation::new(relation)?;
                let scope = rtdb_types::PlacedAttemptScope::decode(&scope)?;
                let control = if name == "rtdb-hosted-catalog-arrow-v1" {
                    let (table, rows) = source_document_rows(body, offset)?;
                    let structs = rows.as_any().downcast_ref::<StructArray>()
                        .filter(|rows| rows.len() == 1).ok_or_else(|| invalid("Hosted catalog needs one document"))?;
                    if table != relation.name() { return Err(invalid("Hosted catalog document has another relation")); }
                    let layout = DecodedDocumentLayout::new(structs.fields(), memory)?;
                    let documents = ArrowDocuments::from_source(structs, layout, source_bytes, memory)?
                        .ok_or_else(|| invalid("Hosted catalog document is null"))?;
                    let presence = read_owned_field_presence(value("field_presence")?)?.unwrap_or_default();
                    let [fields] = presence.as_slice() else { return Err(invalid("Hosted catalog needs one field set")); };
                    let documents = documents.select_owned_fields(fields, memory)?;
                    crate::HostedControlWrite::arrow_catalog(relation, scope, expected, documents)?
                        .ok_or_else(|| invalid("Hosted catalog document has an invalid layout"))?
                } else {
                    let Value::Struct(document) = value("document")? else { return Err(invalid("Hosted control document is not a struct")); };
                    if name == "rtdb-hosted-control-check-v1" {
                        crate::HostedControlWrite::source_check(relation, scope, expected, document)?
                    } else {
                        crate::HostedControlWrite::new(relation, scope, expected, document)?
                    }
                };
                TxOp::HostedControl(control)
            }
            "rtdb-hosted-constraint-refusal-v1" => {
                if value("version")? != Value::I64(1) {
                    return Err(invalid("Invalid hosted constraint receipt version"));
                }
                TxOp::HostedConstraintRefusal
            }
            "rtdb-owned-clock-slot-v4" => {
                let (Value::I64(id), Value::I64(micros), Value::Binary(issued), Value::Bool(funded)) =
                    (value("through")?, value("reading")?, value("issued")?, value("funded")?) else {
                    return Err(invalid("Invalid owned clock slot"));
                };
                if id < 0 || micros <= 0 { return Err(invalid("Invalid owned clock slot")); }
                TxOp::OwnedClockSlot {
                    slot: crate::OwnedClockSlot {
                        through: rtdb_types::TxId(id), reading: Timestamp(micros),
                        issued: crate::DecisionObservation::decode(&issued)?, funded,
                    },
                }
            }
            "rtdb-owned-deadline-v1" | "rtdb-owned-staging-deadline-v1" | "rtdb-owned-physical-deadline-v2" => {
                let Value::I64(until) = value("until")? else { return Err(invalid("Owned deadline is not a microsecond timestamp")); };
                let journal_before = if name == "rtdb-owned-physical-deadline-v2" {
                    let (Value::I64(before), Value::Binary(certificate)) =
                        (value("journal_before")?, value("clock_certificate_sha256")?) else {
                        return Err(invalid("Missing physical journal deadline or clock certificate"));
                    };
                    if until <= 0 || before < until { return Err(invalid("Invalid physical journal deadline")); }
                    let certificate = certificate.as_slice().try_into().map_err(|_| invalid("Invalid clock certificate digest length"))?;
                    Some(rtdb_types::TimedJournalCutoff::new(Timestamp(before), certificate)?)
                } else { None };
                TxOp::OwnedDeadline { until: Timestamp(until), recheck_at_staging: name != "rtdb-owned-deadline-v1", journal_before }
            }
            "rtdb-owned-sweep-expired-v2" => {
                let (Value::Utf8(relation), Value::I64(created), Value::I64(expiry), Value::Binary(scope)) =
                    (value("relation")?, value("created")?, value("expiry")?, value("scope")?) else {
                    return Err(invalid("Owned expiry sweep has incomplete scope"));
                };
                TxOp::OwnedSweepExpired {
                    relation: rtdb_types::OwnedRelation::new(relation)?, id: value("id")?.owned_entity_id()?,
                    created: Timestamp(created), expiry: Timestamp(expiry),
                    scope: rtdb_types::PlacedAttemptScope::decode(&scope)?,
                }
            }
            "rtdb-owned-replay-residue-v1" => {
                let (Value::Utf8(relation), Value::Binary(origin)) = (value("relation")?, value("origin")?) else {
                    return Err(invalid("Owned archive residue has no relation or origin"));
                };
                let from = optional_timestamp(value("_valid_from")?)?.ok_or_else(|| invalid("Owned archive residue has no start"))?;
                let to = optional_timestamp(value("_valid_to")?)?.ok_or_else(|| invalid("Owned archive residue has no end"))?;
                let valid = rtdb_types::Interval::new(from, to)?;
                if valid.is_empty() {
                    return Err(invalid("Owned archive residue has an empty extent"));
                }
                TxOp::OwnedReplayResidue {
                    relation: rtdb_types::OwnedRelation::new(relation)?,
                    id: value("id")?.owned_entity_id()?,
                    origin: rtdb_types::EventOrigin(origin.try_into().map_err(|_| invalid("Owned archive residue origin must contain 32 bytes"))?),
                    valid,
                }
            }
            "rtdb-owned-erase-v2" => {
                let Value::Utf8(relation) = value("relation")? else { return Err(invalid("Owned erase has no relation")); };
                TxOp::OwnedErase { relation: rtdb_types::OwnedRelation::new(relation)?, id: value("id")?.owned_entity_id()? }
            }
            "rtdb-owned-relative-delete-v2" => {
                let Value::Utf8(relation) = value("relation")? else {
                    return Err(invalid("Owned relative deletion requires a relation"));
                };
                TxOp::OwnedRelativePeriod {
                    relation: rtdb_types::OwnedRelation::from_source(relation)?,
                    effect: OwnedPeriodEffect::Delete(value("id")?.owned_entity_id()?),
                    period: read_relative_period(body, offset)?,
                }
            }
            "rtdb-owned-check-v2" | "rtdb-owned-delete-v1" => {
                let Value::Utf8(relation) = value("relation")? else { return Err(invalid("Owned operation has no relation")); };
                let relation = rtdb_types::OwnedRelation::from_source(relation)?;
                let id = value("id")?.owned_entity_id()?;
                if name == "rtdb-owned-check-v2" {
                    let Value::I64(valid_at) = value("valid_at")? else { return Err(invalid("Owned observation has no valid instant")); };
                    let expected = match (value("expected_history")?, value("expected_position")?) {
                        (Value::Null, Value::Null) => None,
                        (Value::I64(time), Value::Binary(position)) => {
                            let position = rtdb_types::EventPosition::from_bytes(position.as_slice().try_into()
                                .map_err(|_| invalid("Owned observation has an invalid event position"))?);
                            if position.transaction.0 < 0 { return Err(invalid("Invalid owned observation transaction")); }
                            Some(rtdb_types::OwnedPointVersion { history: Timestamp(time), position })
                        }
                        _ => return Err(invalid("Owned observation has an invalid revision")),
                    };
                    TxOp::OwnedCheck { relation, id, valid_at: Timestamp(valid_at), expected }
                } else {
                    TxOp::OwnedDelete { relation, id, valid_from: optional_timestamp(value("_valid_from")?)?, valid_to: optional_timestamp(value("_valid_to")?)? }
                }
            }
            "sql" => {
                let sql = match value("query")? { Value::Utf8(sql) => sql, _ => return Err(invalid("SQL operation has no query")) };
                let arguments = body.fields().find("args").and_then(|(index, field)| {
                    (field.data_type() == &DataType::Binary && field.metadata().is_empty())
                        .then(|| body.column(index).as_any().downcast_ref::<arrow_array::BinaryArray>()).flatten()
                }).filter(|array| array.is_valid(offset));
                let args = if let Some(array) = arguments {
                    let offsets = array.value_offsets();
                    let start = offsets[offset] as usize;
                    let length = offsets[offset + 1] as usize - start;
                    let bytes = array.values().slice_with_length(start, length);
                    Some(decode_arguments(bytes, memory)?)
                } else {
                    match value("args")? {
                        Value::Null | Value::Undefined => None,
                        Value::Binary(bytes) => Some(decode_arguments(Buffer::from_vec(bytes), memory)?),
                        _ => return Err(invalid("SQL arguments are not IPC bytes")),
                    }
                };
                TxOp::Sql { sql, args }
            }
            "rtdb-owned-insert-v1" | "rtdb-owned-replace-v1"
            | "rtdb-owned-relative-insert-v2" | "rtdb-owned-relative-replace-v2"
            | "insert-docs" | "put-docs" | "patch-docs" => {
                let (table, rows) = source_document_rows(body, offset)?;
                let relative = matches!(name.as_str(), "rtdb-owned-relative-insert-v2" | "rtdb-owned-relative-replace-v2");
                let owned = (relative || matches!(name.as_str(), "rtdb-owned-insert-v1" | "rtdb-owned-replace-v1"))
                    .then(|| rtdb_types::OwnedRelation::from_source(table.clone())).transpose()?;
                let table = if owned.is_none() { Some(parse_table(&table)?) } else { None };
                let structs = rows.as_any().downcast_ref::<StructArray>().ok_or_else(|| invalid("Document is not a struct"))?;
                let (valid_from, valid_to) =
                    (optional_timestamp(value("_valid_from")?)?, optional_timestamp(value("_valid_to")?)?);
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
                    let iids = iids.ok_or_else(|| invalid("Owned insert operation has no IIDs"))?;
                    if relation.is_root_data() && matches!(value("arrow_payload"), Ok(Value::Bool(true))) {
                        let presence = presence.ok_or_else(|| invalid("Owned Arrow payload requires exact field presence"))?;
                        let documents = ArrowDocuments::from_source(structs, Arc::clone(&layout), source_bytes, memory)?
                            .ok_or_else(|| invalid("Owned Arrow payload is not a struct"))?;
                        if relative {
                            let period = read_relative_period(body, offset)?;
                            let ([fields], [iid]) = (presence.as_slice(), iids.as_slice()) else {
                                return Err(invalid("Relative Arrow writes require one fact"));
                            };
                            let documents = documents.select_owned_fields(fields, memory)?;
                            if documents.owned_id_ref(0)?.iid() != *iid {
                                return Err(invalid("Owned document IID does not match its identity"));
                            }
                            let effect = OwnedPeriodEffect::Arrow { documents, create_only: name == "rtdb-owned-relative-insert-v2" };
                            return Ok(Operation::Public(TxOp::OwnedRelativePeriod { relation, effect, period }));
                        }
                        return Ok(Operation::OwnedArrowIids { create_only: name == "rtdb-owned-insert-v1", relation, documents, iids,
                            presence: Some(presence), valid_from, valid_to });
                    }
                    if (relation.is_root_data() || (!relative && (relation.world().is_some() || relation.name() == rtdb_types::OWNED_ROOT_PUBLICATION_RELATION)))
                        && let Some(documents) = legacy_owned_arrow_documents(structs, &layout, source_bytes, presence.as_deref(), memory)?
                    {
                        if (0..documents.row_count()).all(|row| {
                            presence.as_ref().is_none_or(|fields| fields[row].contains("_id"))
                                && documents.owned_id_ref(row).is_ok()
                        }) {
                            let documents = documents.with_legacy_owned_presence(presence, memory)?;
                            if relative {
                                let period = read_relative_period(body, offset)?;
                                for (row, iid) in iids.iter().enumerate() {
                                    if documents.owned_id_ref(row)?.iid() != *iid {
                                        return Err(invalid("Owned document IID does not match its identity"));
                                    }
                                }
                                let effect = OwnedPeriodEffect::Arrow { documents, create_only: name == "rtdb-owned-relative-insert-v2" };
                                return Ok(Operation::Public(TxOp::OwnedRelativePeriod { relation, effect, period }));
                            }
                            return Ok(Operation::OwnedArrowIids { create_only: name == "rtdb-owned-insert-v1", relation, documents, iids,
                                presence: None, valid_from, valid_to });
                        }
                    }
                    let documents = Documents::read_arrow_with_presence(structs, layout, true, presence.as_deref(), memory)?;
                    if relative {
                        let period = read_relative_period(body, offset)?;
                        for (row, iid) in documents.rows().iter().zip(iids) {
                            if owned_document_id(row, memory)?.iid() != iid {
                                return Err(invalid("Owned document IID does not match its identity"));
                            }
                        }
                        let effect = if name == "rtdb-owned-relative-insert-v2" {
                            OwnedPeriodEffect::Create(documents)
                        } else {
                            OwnedPeriodEffect::Replace(documents)
                        };
                        return Ok(Operation::Public(TxOp::OwnedRelativePeriod {
                            relation, effect, period,
                        }));
                    }
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

fn argument_column(
    arguments: &[Option<i32>], values: Option<crate::ipc::Output>, arrays: &mut BatchBuilder,
) -> Result<ArrayRef> {
    let bound = arguments.len().checked_mul(16)
        .and_then(|size| size.checked_add(values.as_ref().map_or(0, crate::ipc::Output::len).checked_mul(2)?))
        .and_then(|size| size.checked_add(4096)).ok_or_else(overflow)?;
    arrays.column(bound, || {
        let mut offsets = Vec::with_capacity(arguments.len() + 1);
        let mut nulls = NullBufferBuilder::new(arguments.len());
        offsets.push(0);
        let mut end = 0;
        for &argument in arguments {
            if let Some(offset) = argument { end = offset; }
            offsets.push(end);
            nulls.append(argument.is_some());
        }
        let (bytes, _reservation) = values.map(crate::ipc::Output::into_parts).unzip();
        let values = Buffer::from_vec(bytes.unwrap_or_default().into_boxed_slice().into_vec());
        let array = BinaryArray::try_new(OffsetBuffer::new(offsets.into()), values, nulls.finish())
            .map_err(|error| invalid(error.to_string()))?;
        Ok(Arc::new(array) as ArrayRef)
    })
}

pub(crate) fn encode_arguments(arguments: &SqlArguments, memory: &Arc<MemoryBudget>) -> Result<RetainedBytes> {
    let mut output = crate::ipc::Output::new(memory, 0)?;
    write_arguments(arguments, memory, SourcePurpose::Receipt, &mut output)?;
    let (bytes, reservation) = output.into_parts();
    RetainedBytes::with_reservation(bytes, reservation)
}

fn argument_output<'a>(
    output: &'a mut Option<crate::ipc::Output>, memory: &Arc<MemoryBudget>,
) -> Result<&'a mut crate::ipc::Output> {
    Ok(match output {
        Some(output) => output,
        None => output.insert(crate::ipc::Output::new(memory, 0)?),
    })
}

fn write_arguments(
    arguments: &SqlArguments, memory: &Arc<MemoryBudget>, purpose: SourcePurpose, output: &mut crate::ipc::Output,
) -> Result<()> {
    if arguments.retains_uploaded_source_layout() && let Some(pages) = arguments.uniform_arrow_pages() {
        let first = pages[0];
        let _schema = memory.reserve(rtdb_format::ipc::schema_translation_bound(first.schema_ref())?)?;
        let fields = first.schema_ref().fields().iter().enumerate().map(|(index, field)| {
            field.as_ref().clone().with_name(format!("?_{index}"))
        }).collect::<Vec<_>>();
        let schema = Arc::new(Schema::new(fields));
        let mut populated = pages.iter().copied().filter(|page| page.num_rows() != 0);
        let single = if pages.len() == 1 {
            Some(first)
        } else if purpose == SourcePurpose::Log
            && let Some(page) = populated.next()
            && populated.next().is_none()
            && page.schema_ref().fields().iter().zip(page.columns()).all(|(field, column)| {
                field.data_type().is_numeric() && field.metadata().is_empty() && column.null_count() == 0
            })
        {
            Some(page)
        } else {
            None
        };
        if single.is_none()
            && let Some(spans) = rtdb_format::ipc::RecordSpans::concat(&pages, 8, memory)?
        {
            return spans.write_java_stream(&schema, output);
        }
        let _reservation = memory.reserve(pages.iter().try_fold(4096usize, |bytes, page| {
            page.get_array_memory_size().checked_mul(4).and_then(|size| bytes.checked_add(size)).ok_or_else(overflow)
        })?)?;
        let combined;
        let page = if let Some(single) = single {
            single
        } else {
            // Replay uses IPC page boundaries for SQL error indices and single-page execution.
            combined = arrow_select::concat::concat_batches(first.schema_ref(), pages).map_err(|error| invalid(error.to_string()))?;
            &combined
        };
        let options = arrow_array::RecordBatchOptions::new().with_row_count(Some(page.num_rows()));
        let page = RecordBatch::try_new_with_options(Arc::clone(&schema), page.columns().to_vec(), &options)
            .map_err(|error| invalid(error.to_string()))?;
        return output.write_stream(&schema, [&page]);
    }
    let batch = arguments.source_columnar_batch(memory)?;
    output.write_stream(batch.batch().schema_ref(), [batch.batch()])
}

pub(crate) fn decode_arguments(bytes: impl Into<Buffer>, memory: &Arc<MemoryBudget>) -> Result<SqlArguments> {
    let bytes = bytes.into();
    let _value_reservation = memory.reserve(crate::ipc::allocation_bound(&bytes)?)?;
    let stream = rtdb_format::ipc::OwnedStream::read_buffer(bytes, memory)?;
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
    let owned = metadata_batch(metadata, memory)?;
    crate::ipc::encode(owned.batch().schema_ref(), [owned.batch()], memory)
}

fn metadata_batch(metadata: &UserMetadata, memory: &Arc<MemoryBudget>) -> Result<OwnedBatch> {
    let ty = metadata.value().logical_type().maybe();
    let reservation = memory.reserve(estimate_builder_bytes(&ty, std::slice::from_ref(metadata.value()))?)?;
    let batch = RecordBatch::try_new(Arc::new(Schema::new(vec![to_field("user-metadata", &ty)?])), vec![build_array(&ty, std::slice::from_ref(metadata.value()))?]).map_err(|error| invalid(error.to_string()))?;
    OwnedBatch::with_reservation(batch, reservation)
}
fn decode_metadata(bytes: &Bytes, memory: &Arc<MemoryBudget>) -> Result<UserMetadata> {
    let _value_reservation = memory.reserve(crate::ipc::allocation_bound(bytes)?)?;
    let pages = crate::ipc::decode(bytes, memory)?;
    if pages.len() != 1 || pages[0].batch().num_rows() != 1 { return Err(invalid("User metadata must contain one row")); }
    UserMetadata::new(cell(pages[0].batch(), "user-metadata", 0)?, memory)
}
fn source_document_rows(body: &StructArray, row: usize) -> Result<(String, ArrayRef)> {
    let documents = body.column_by_name("documents").ok_or_else(|| invalid("Document operation has no documents"))?;
    let tables = documents.as_any().downcast_ref::<UnionArray>().ok_or_else(|| invalid("Document tables are not a union"))?;
    let (table, array, offset) = union_leg(tables, row)?;
    let lists = array.as_any().downcast_ref::<ListArray>().ok_or_else(|| invalid("Documents are not a list"))?;
    if lists.is_null(offset) { return Err(invalid("Documents are not a list")); }
    Ok((table, lists.value(offset)))
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
fn read_relative_period(body: &StructArray, row: usize) -> Result<OwnedRelativePeriod> {
    let value = |name| struct_cell(body, name, row);
    let offset = |name| match value(name)? {
        Value::I64(micros) => Ok(Some(Timestamp(micros))),
        Value::Null => Ok(None),
        _ => Err(invalid("Relative period offsets must be signed microseconds")),
    };
    OwnedRelativePeriod::from_fields(
        optional_timestamp(value("_valid_from")?)?,
        optional_timestamp(value("_valid_to")?)?,
        offset("from_offset_micros")?,
        offset("to_offset_micros")?,
    )
    .ok_or_else(|| invalid("Invalid original relative period bounds"))
}

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
        Value::List(values) => values.into_values().into_iter().map(|value| match value {
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
        Operation::Public(TxOp::InsertDocs { table, .. } | TxOp::PutDocs { table, .. } | TxOp::PutArrowDocs { table, .. } | TxOp::WriteArrowDocs { table, .. } | TxOp::PatchDocs { table, .. } | TxOp::DeleteDocs { table, .. } | TxOp::EraseDocs { table, .. })
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
    fn journal_source_bytes_match_generated_protobuf_for_ordinary_and_hosted_guards() {
        use rtdb_format::generated::xtdb::log::proto;
        use rtdb_types::PlacedAttemptScope;

        let memory = budget();
        let nonce = "0123456789abcdef0123456789abcdef";
        let seed = PlacedAttemptScope::new("history", 7).unwrap().issue();
        let ordinary = SourceTransaction::new(Vec::new(), SubmitOptions::default(), &memory).unwrap();
        let application = SourceTransaction::new(vec![TxOp::Sql {
            sql: format!("ASSERT TRUE /*{}*/", "x".repeat(16_384)), args: None,
        }], SubmitOptions { user: Some("writer".into()), ..Default::default() }, &memory).unwrap()
            .with_idempotency_guard(Timestamp(-1), false);
        let relation = rtdb_types::OwnedRelation::new("facts").unwrap();
        let batch = RecordBatch::try_from_iter([
            ("_id", Arc::new(arrow_array::StringArray::from(vec!["a", "b"])) as ArrayRef),
            ("payload", Arc::new(arrow_array::BinaryArray::from(vec![Some(b"data".as_slice()), None]))),
        ]).unwrap();
        let batch = OwnedBatch::new(batch, &memory).unwrap();
        let documents = ArrowDocuments::from_owned_payload(&batch, &memory).unwrap().unwrap();
        let hosted_ops = || vec![
            TxOp::OwnedDelete {
                relation: relation.clone(), id: EntityId::String("first".into()), valid_from: None, valid_to: None,
            },
            TxOp::OwnedArrowDocs {
                relation: relation.clone(), documents: documents.clone(), create_only: true, valid_from: None, valid_to: None,
            },
            TxOp::OwnedDelete {
                relation: relation.clone(), id: EntityId::String("last".into()), valid_from: None, valid_to: None,
            },
        ];
        let replay = ReplaySubmission::admit(&format!("{nonce}/0"), Timestamp(1), Timestamp(2)).unwrap();
        let hosted = SourceTransaction::new(hosted_ops(), SubmitOptions::default(), &memory).unwrap()
            .with_replay_submission(replay).with_placed_seed(Some(seed));
        let bounded_metadata = UserMetadata::new(Value::Struct(StructFields::from([
            (BOUNDED_RETRY_METADATA_KEY.into(), Value::Struct(StructFields::from([
                ("nonce".into(), Value::Utf8(nonce.into())), ("not_after_micros".into(), Value::I64(2)),
            ]))),
        ])), &memory).unwrap();
        let bounded = SourceTransaction::new(hosted_ops(), SubmitOptions {
            user: Some("owner".into()), user_metadata: Some(bounded_metadata), ..Default::default()
        }, &memory).unwrap().with_placed_seed(Some(seed));

        for (source, mut tx, legacy_abort) in [
            (ordinary, proto::Tx::default(), false),
            (application, proto::Tx {
                user: Some("writer".into()), idempotency_guard: Some(proto::IdempotencyGuard {
                    submitted_at_micros: Some(-1), after_bootstrap: Some(false),
                }), ..Default::default()
            }, false),
            (hosted, proto::Tx {
                replay_submission: Some(proto::ReplaySubmission {
                    key: Some(format!("{nonce}/0")), submitted_at_micros: Some(1),
                    not_after_micros: Some(2), abort_on_legacy_resolver: Some(true),
                }), placed_attempt_seed: Some(seed.encode().to_vec().into()), ..Default::default()
            }, true),
            (bounded, proto::Tx {
                user: Some("owner".into()), bounded_retry: Some(proto::BoundedRetry {
                    nonce: Some(nonce.as_bytes().to_vec()), not_after_micros: Some(2),
                }), placed_attempt_seed: Some(seed.encode().to_vec().into()), ..Default::default()
            }, true),
        ] {
            let legacy_assertion = legacy_abort.then(|| Operation::Public(TxOp::Sql {
                sql: "ASSERT FALSE".into(), args: None,
            }));
            let batch = source.operation_batch(legacy_assertion.as_ref(), SourcePurpose::Log).unwrap();
            let ipc = rtdb_format::ipc::write_stream_to(batch.batch().schema_ref(), [batch.batch()], Vec::new()).unwrap();
            tx.tx_ops = Some(ipc.into());
            tx.default_tz = Some("UTC".into());
            tx.user_metadata = source.options.user_metadata.as_ref().map(|metadata| {
                Bytes::copy_from_slice(encode_metadata(metadata, &memory).unwrap().as_slice())
            });
            let expected = SourceMessage::Protobuf(SourceBody::Tx(tx)).encode().unwrap();

            let base_charge = memory.used_bytes();
            let actual = source.encode().unwrap();
            assert_eq!(actual.as_slice(), expected.as_slice());
            let address = actual.as_slice().as_ptr();
            let bytes = actual.into_bytes();
            assert_eq!(bytes.as_ptr(), address);
            let retained_charge = memory.used_bytes();
            assert!(retained_charge >= base_charge + bytes.len());
            let message = SourceMessage::decode_bytes(bytes.clone()).unwrap().unwrap();
            drop(bytes);
            assert_eq!(memory.used_bytes(), retained_charge);

            let replay = SourceTransaction::decode(&message, &memory).unwrap();
            assert_eq!(replay.operations.len(), source.operations.len());
            if legacy_abort {
                let [Operation::Public(TxOp::OwnedDelete { id: first, .. }),
                    Operation::OwnedArrowIids { documents, iids, create_only: true, .. },
                    Operation::Public(TxOp::OwnedDelete { id: last, .. })] = replay.operations.as_slice() else {
                    panic!("expected hosted operations in their submitted order");
                };
                assert_eq!(first, &EntityId::String("first".into()));
                assert_eq!(last, &EntityId::String("last".into()));
                assert_eq!(iids, &[EntityId::String("a".into()).iid(), EntityId::String("b".into()).iid()]);
                assert!(documents.array().column(1).is_null(1));
            }
            assert_eq!(replay.placed_seed, source.placed_seed);
            drop((replay, message));
            assert_eq!(memory.used_bytes(), base_charge);
        }
    }

    #[test]
    fn replay_residue_source_keeps_original_origin_identity_and_extent() {
        let memory = budget();
        let relation = rtdb_types::OwnedRelation::new("facts").unwrap();
        let id = EntityId::Uuid([1; 16]);
        let origin = rtdb_types::EventOrigin::at([2; 16], 5, rtdb_types::BlockIndex(3));
        let valid = rtdb_types::Interval::new(Timestamp(10), Timestamp(20)).unwrap();
        let source = SourceTransaction::new(vec![TxOp::OwnedReplayResidue {
            relation: relation.clone(), id: id.clone(), origin, valid,
        }], SubmitOptions::default(), &memory).unwrap();
        assert!(matches!(roundtrip(&source).operations.as_slice(), [
            Operation::Public(TxOp::OwnedReplayResidue { relation: r, id: i, origin: o, valid: v })
        ] if (r, i, *o, *v) == (&relation, &id, origin, valid)));
    }

    #[test]
    fn relative_root_period_keeps_offsets_until_ordering() {
        let relation = rtdb_types::OwnedRelation::new("facts").unwrap();
        let id = EntityId::String("entity".into());
        let memory = budget();
        let documents = Documents::new(vec![StructFields::from([("_id".into(), id.as_value())])], &memory).unwrap();
        let relative = |from, to, from_offset, to_offset| {
            OwnedRelativePeriod::from_fields(from, to, from_offset, to_offset).unwrap()
        };
        let source = SourceTransaction::new(vec![
            TxOp::OwnedRelativePeriod {
                relation: relation.clone(), effect: OwnedPeriodEffect::Replace(documents),
                period: relative(None, None, Some(Timestamp(-7)), Some(Timestamp(3))),
            },
            TxOp::OwnedRelativePeriod {
                relation: relation.clone(), effect: OwnedPeriodEffect::Delete(id.clone()),
                period: relative(None, Some(Timestamp(133)), Some(Timestamp(8)), None),
            },
            TxOp::OwnedRelativePeriod {
                relation, effect: OwnedPeriodEffect::Delete(id),
                period: relative(Some(Timestamp(130)), None, None, Some(Timestamp(10))),
            },
        ], SubmitOptions::default(), &memory).unwrap();
        let mut replayed = roundtrip(&source);
        assert!(matches!(&replayed.operations[0], Operation::Public(TxOp::OwnedRelativePeriod {
            period, ..
        }) if *period == relative(None, None, Some(Timestamp(-7)), Some(Timestamp(3)))));
        assert!(replayed.place_owned_periods(Timestamp(123), false).is_err());
        replayed.place_owned_periods(Timestamp(123), true).unwrap();
        assert!(matches!(&replayed.operations[0], Operation::Public(TxOp::OwnedArrowDocs {
            create_only: false, valid_from: Some(Timestamp(116)), valid_to: Some(Timestamp(126)), ..
        })));
        assert!(matches!(&replayed.operations[1], Operation::Public(TxOp::OwnedDelete {
            valid_from: Some(Timestamp(131)), valid_to: Some(Timestamp(133)), ..
        })));
        assert!(matches!(&replayed.operations[2], Operation::Public(TxOp::OwnedDelete {
            valid_from: Some(Timestamp(130)), valid_to: Some(Timestamp(133)), ..
        })));
        assert!(roundtrip(&source).place_owned_periods(Timestamp(125), true).is_err());
        assert!(roundtrip(&source).place_owned_periods(Timestamp(i64::MAX), true).is_err());
        assert!(OwnedRelativePeriod::from_fields(
            Some(Timestamp(1)), None, Some(Timestamp(2)), Some(Timestamp(3)),
        ).is_none());
    }

    #[test]
    fn relative_root_arrow_replay_keeps_geometry_and_exact_presence() {
        let memory = budget();
        let relation = rtdb_types::OwnedRelation::new("locations").unwrap();
        let arrow = |id: &str, column, bytes: &[u8]| {
            let batch = RecordBatch::try_from_iter([
                ("_id", Arc::new(arrow_array::StringArray::from(vec![id])) as arrow_array::ArrayRef),
                (column, Arc::new(arrow_array::BinaryArray::from_iter_values([bytes]))),
            ]).unwrap();
            let batch = OwnedBatch::new(batch, &memory).unwrap();
            let documents = ArrowDocuments::from_owned_payload(&batch, &memory).unwrap().unwrap();
            TxOp::OwnedRelativePeriod {
                relation: relation.clone(), effect: OwnedPeriodEffect::Arrow { documents, create_only: false },
                period: OwnedRelativePeriod::from_fields(None, Some(Timestamp(126)), Some(Timestamp(-7)), None).unwrap(),
            }
        };
        let transaction = SourceTransaction::new(vec![
            arrow("a", "location", b"wkb"), arrow("b", "partition", b"zone"),
        ], SubmitOptions::default(), &memory).unwrap();
        let mut replayed = roundtrip(&transaction);
        replayed.place_owned_periods(Timestamp(123), true).unwrap();
        let Operation::Public(TxOp::OwnedArrowDocs {
            documents, create_only: false, valid_from: Some(Timestamp(116)), valid_to: Some(Timestamp(126)), ..
        }) = &replayed.operations[0] else { panic!("expected placed Arrow write"); };
        assert_eq!(documents.columns(), ["_id", "location"]);
        let payload = documents.array().column(1).as_any().downcast_ref::<arrow_array::BinaryArray>().unwrap();
        assert_eq!(payload.value(0), b"wkb");
    }

    #[test]
    fn hosted_catalog_source_retains_arrow_payload_and_exact_field_presence() {
        use arrow_array::{BinaryArray, Int64Array, StringArray};
        use rtdb_types::PlacedAttemptScope;

        let memory = budget();
        let scope = PlacedAttemptScope::new("hosted-catalog", 1).unwrap();
        let declarations = rtdb_types::OwnedRelation::new("bj_relation_declarations").unwrap();
        let payload = vec![0x9a; 700 * 1024];
        let write = |id, history_days: Option<i64>| {
            let mut columns = vec![
                ("_id", Arc::new(StringArray::from(vec![id])) as ArrayRef),
                ("declaration", Arc::new(BinaryArray::from_iter_values([payload.as_slice()])) as ArrayRef),
            ];
            if let Some(days) = history_days {
                columns.push(("history_days", Arc::new(Int64Array::from(vec![days]))));
            }
            let batch = OwnedBatch::new(RecordBatch::try_from_iter(columns).unwrap(), &memory).unwrap();
            let docs = ArrowDocuments::from_owned_payload(&batch, &memory).unwrap().unwrap();
            TxOp::HostedControl(crate::HostedControlWrite::arrow_catalog(
                declarations.clone(), scope, Some(Timestamp(11)), docs,
            ).unwrap().unwrap())
        };
        let legacy = crate::HostedControlWrite::new(
            rtdb_types::OwnedRelation::new("bj_fact_root_program").unwrap(), scope, None,
            StructFields::from([("_id".into(), Value::Utf8("active".into())), ("generation".into(), Value::I64(7))]),
        ).unwrap();
        let tx = SourceTransaction::new(vec![write("with-retention", Some(7)), write("without-retention", None),
            TxOp::HostedControl(legacy)], SubmitOptions::default(), &memory).unwrap();
        let restored = roundtrip(&tx);
        for (i, id) in ["with-retention", "without-retention"].into_iter().enumerate() {
            let Operation::Public(TxOp::HostedControl(control)) = &restored.operations[i] else { panic!("expected catalog control"); };
            let documents = control.arrow_replacement().unwrap();
            assert_eq!(control.id(), &rtdb_types::EntityId::String(id.into()));
            assert_eq!(control.expected(), Some(Timestamp(11)));
            assert_eq!(control.scope(), scope);
            assert_eq!(documents.columns().contains(&"history_days".to_owned()), i == 0);
            let bytes = documents.array().column_by_name("declaration").unwrap().as_any().downcast_ref::<BinaryArray>().unwrap();
            assert_eq!(bytes.value(0), payload);
        }
        let Operation::Public(TxOp::HostedControl(legacy)) = &restored.operations[2] else { panic!("expected legacy control"); };
        assert_eq!(legacy.replacement().unwrap().get("generation"), Some(&Value::I64(7)));
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
    fn owned_path_columns_preserve_the_source_operation_layout() {
        let memory = MemoryBudget::new(1 << 20);
        let locations = ["location".to_owned(), "borough".to_owned()];
        for columns in [None, Some(&[][..]), Some(locations.as_slice())] {
            let mut legacy = StructFields::from([
                ("relation".into(), Value::Utf8("places".into())),
                ("name".into(), Value::Utf8("by_location".into())),
            ]);
            if let Some(columns) = columns {
                legacy.insert("columns".into(), Value::list(columns.iter().map(|column| Value::Utf8(column.as_str().into())).collect()));
            }
            let operation_array = |body| {
                let mut arrays = BatchBuilder::new(4096, &memory).unwrap();
                operation_leg(vec![SourceOperation {
                    body,
                    iids: Cow::Borrowed(&[]),
                    documents: None,
                }], &mut arrays, &memory, &mut None).unwrap()
            };
            let expected = operation_array(SourceOperationBody::Values { fields: legacy, arguments: None });
            let actual = operation_array(SourceOperationBody::Path { relation: "places", name: "by_location", columns, encoding: None });
            assert_eq!(actual.to_data(), expected.to_data());
        }
    }

    #[test]
    fn a_spatial_readiness_condition_replays_with_its_exact_definition() {
        let memory = budget();
        let relation = rtdb_types::OwnedRelation::new("places").unwrap();
        let path = rtdb_types::OwnedRelation::new("by_location").unwrap();
        let columns = vec!["location".to_owned(), "area".to_owned()];
        let source = SourceTransaction::new(vec![TxOp::OwnedRequireSpatialPath {
            relation: relation.clone(), name: path.clone(), columns: columns.clone(),
            encoding: rtdb_types::SpatialGeometryEncoding::BijectionPosition,
        }], SubmitOptions::default(), &memory).unwrap();
        let replayed = roundtrip(&source);
        assert!(matches!(replayed.operations.as_slice(), [Operation::Public(TxOp::OwnedRequireSpatialPath {
            relation: actual, name, columns: actual_columns, encoding: rtdb_types::SpatialGeometryEncoding::BijectionPosition,
        })] if actual == &relation && name == &path && actual_columns == &columns));
    }

    #[test]
    fn a_relation_retirement_replays_with_its_retention() {
        let memory = budget();
        let relation = rtdb_types::OwnedRelation::new("orders").unwrap();
        let source = SourceTransaction::new(vec![TxOp::OwnedRetireRelation {
            relation: relation.clone(), retain_micros: 86_400_000_000,
        }], SubmitOptions::default(), &memory).unwrap();
        let replayed = roundtrip(&source);
        assert!(matches!(replayed.operations.as_slice(), [Operation::Public(TxOp::OwnedRetireRelation {
            relation: actual, retain_micros: 86_400_000_000,
        })] if actual == &relation));
    }

    #[test]
    fn a_relation_adoption_replays_with_its_exact_endpoints_and_retention() {
        let memory = budget();
        let original = rtdb_types::OwnedRelation::new("fact_original").unwrap();
        let successor = rtdb_types::OwnedRelation::new("fact_successor").unwrap();
        let source = SourceTransaction::new(vec![TxOp::OwnedAdoptRelation {
            source: original.clone(), destination: successor.clone(), retain_micros: 5000,
        }], SubmitOptions::default(), &memory).unwrap();
        let replayed = roundtrip(&source);
        assert!(replayed.requires_durable_predecessor());
        assert!(matches!(replayed.operations.as_slice(), [Operation::Public(TxOp::OwnedAdoptRelation {
            source, destination, retain_micros: 5000,
        })] if source == &original && destination == &successor));
        assert_eq!(source.body_digest().unwrap(), replayed.body_digest().unwrap());
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
        assert_eq!(source.encode().unwrap().as_slice(), replayed.encode().unwrap().as_slice());

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
    fn sql_source_adopts_the_only_argument_stream_with_canonical_bytes_and_null_offsets() {
        let memory = budget();
        let schema = Arc::new(Schema::new(vec![Field::new("client_value", DataType::Float64, true)]));
        let values = arrow_array::Float64Array::from(vec![1.0, -0.0, f64::from_bits(0x7ff8_0000_0000_0007), 3.0]);
        let page = RecordBatch::try_new(schema, vec![Arc::new(values)]).unwrap().slice(1, 2);
        let upload = |page| SqlArguments::from_arrow(OwnedBatch::new(page, &memory).unwrap(), &memory).unwrap();
        let mut arguments = upload(page.slice(0, 0));
        arguments.append(upload(page.clone())).unwrap();
        arguments.append(upload(page.slice(2, 0))).unwrap();
        let arg_bytes = encode_arguments(&arguments, &memory).unwrap();

        let sql = "UPDATE docs SET v = ?";
        let request = |bound| {
            SourceTransaction::new(vec![
                TxOp::Sql { sql: "ASSERT TRUE".into(), args: None },
                bound,
                TxOp::Sql { sql: "ASSERT TRUE".into(), args: None },
            ], SubmitOptions::default(), &memory).unwrap()
        };
        let legacy = request(TxOp::SqlBytes { sql: sql.into(), arg_bytes });
        let source = request(TxOp::Sql { sql: sql.into(), args: Some(arguments) });
        assert_eq!(source.encode().unwrap().as_slice(), legacy.encode().unwrap().as_slice());
        assert_eq!(source.body_digest().unwrap(), legacy.body_digest().unwrap());

        let replayed = roundtrip(&source);
        assert_eq!(source.body_digest().unwrap(), replayed.body_digest().unwrap());
        let Operation::Public(TxOp::Sql { args: Some(arguments), .. }) = &replayed.operations[1] else {
            panic!("Expected SQL arguments");
        };
        assert_eq!(arguments.single_arrow_page().unwrap().batch().num_rows(), 2);
        assert_eq!(arguments.indexed_rows().map(|row| row.unwrap().0).collect::<Vec<_>>(), [0, 1]);
    }

    #[test]
    fn sql_source_packs_multiple_argument_streams_in_operation_order() {
        let memory = budget();
        let submitted = |value| SqlArguments::from_rows(vec![vec![Value::I64(value)]], &memory).unwrap();
        let bytes = |value| encode_arguments(&submitted(value), &memory).unwrap();
        let sql = "UPDATE docs SET v = ?";
        let request = |first, last| SourceTransaction::new(vec![
            first,
            TxOp::Sql { sql: "ASSERT TRUE".into(), args: None },
            TxOp::SqlBytes { sql: sql.into(), arg_bytes: bytes(2) },
            last,
        ], SubmitOptions::default(), &memory).unwrap();
        let source = request(
            TxOp::Sql { sql: sql.into(), args: Some(submitted(1)) },
            TxOp::Sql { sql: sql.into(), args: Some(submitted(3)) },
        );
        let reference = request(
            TxOp::SqlBytes { sql: sql.into(), arg_bytes: bytes(1) },
            TxOp::SqlBytes { sql: sql.into(), arg_bytes: bytes(3) },
        );
        assert_eq!(source.encode().unwrap().as_slice(), reference.encode().unwrap().as_slice());
        assert_eq!(source.body_digest().unwrap(), reference.body_digest().unwrap());
        let replayed = roundtrip(&source);
        let values: Vec<_> = replayed.operations.iter().filter_map(|operation| match operation {
            Operation::Public(TxOp::Sql { args: Some(arguments), .. }) => {
                Some(arguments.rows().next().unwrap().unwrap()[0].clone())
            }
            _ => None,
        }).collect();
        assert_eq!(values, [Value::I64(1), Value::I64(2), Value::I64(3)]);
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
        let a = Value::list(vec![Value::I64(7)]);
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
        let replayed = decode_arguments(bytes.into_bytes(), &memory).unwrap();

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
        let first = [Value::list(vec![Value::I64(4), Value::Null]), Value::FixedSizeList(vec![Value::Null, Value::Utf8("x".into())].into())];
        let second = [Value::list(vec![Value::Null]), Value::FixedSizeList(vec![Value::I64(5), Value::Null].into())];
        let mut arguments = chunk(["left_list", "left_pair"], &first);
        arguments.append(chunk(["right_list", "right_pair"], &second)).unwrap();
        assert!(arguments.uniform_arrow_pages().is_none());
        assert_eq!(arguments.source_columnar_batch(&memory).unwrap().batch().num_rows(), 2);

        let bytes = encode_arguments(&arguments, &memory).unwrap();
        let replayed = decode_arguments(bytes.into_bytes(), &memory).unwrap();

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
        assert!(bytes.as_slice().len() < payload.len());
        let replayed = decode_arguments(bytes.into_bytes(), &memory).unwrap();

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
    fn submitted_arrow_document_writes_keep_row_source_bytes_and_receipts() {
        let memory = budget();
        let table = TableRef::new("public", "docs");
        let first = Documents::new(vec![
            StructFields::from([("_id".into(), Value::I64(1)), ("vec".into(), Value::list(vec![Value::F64(-0.0), Value::Null]))]),
            StructFields::from([("_id".into(), Value::I64(2))]),
        ], &memory).unwrap();
        let second = Documents::new(vec![
            StructFields::from([("_id".into(), Value::I64(3)), ("vec".into(), Value::list(vec![Value::F32(-0.0), Value::F64(f64::from_bits(0x7ff8_0000_0000_0007))]))]),
            StructFields::from([("_id".into(), Value::I64(4)), ("vec".into(), Value::Null)]),
        ], &memory).unwrap();
        let submitted_first = ArrowDocuments::from_submitted_rows(&first, &memory).unwrap();
        let submitted_second = ArrowDocuments::from_submitted_rows(&second, &memory).unwrap();
        let layouts = [SourceDocuments::Arrow(&submitted_first), SourceDocuments::Arrow(&submitted_second)];
        let rows = [SourceDocuments::Rows(&first), SourceDocuments::Rows(&second)];
        let mut arrays = BatchBuilder::new(4096, &memory).unwrap();
        let expected = canonical_document_struct(&rows, 4, &mut arrays, &memory).unwrap();
        let (field, expected) = document_column(expected.as_struct(), "vec").unwrap();
        let (actual, _lease) = coalesce_submitted_numeric_lists(&layouts, field, &memory).unwrap().expect("typed coalescing");
        assert_eq!(actual.to_data(), expected.to_data());
        let valid_from = Some(Timestamp(123));
        let valid_to = Some(Timestamp(456));

        for write in [ArrowDocumentWrite::Put, ArrowDocumentWrite::Patch] {
            let legacy = |docs: &Documents| match write {
                ArrowDocumentWrite::Put => TxOp::PutDocs { table: table.clone(), documents: docs.clone(), valid_from, valid_to },
                ArrowDocumentWrite::Patch => TxOp::PatchDocs { table: table.clone(), documents: docs.clone(), valid_from, valid_to },
            };
            let arrow = |docs: &Documents| TxOp::WriteArrowDocs {
                table: table.clone(), documents: ArrowDocuments::from_submitted_rows(docs, &memory).unwrap(), write, valid_from, valid_to,
            };
            for pages in [&[&first][..], &[&first, &second][..]] {
                let build = |ops| SourceTransaction::new(ops, SubmitOptions::default(), &memory).unwrap();
                let old = build(pages.iter().map(|docs| legacy(docs)).collect());
                let new = build(pages.iter().map(|docs| arrow(docs)).collect());
                assert_eq!(new.encode().unwrap().as_slice(), old.encode().unwrap().as_slice());
                assert_eq!(new.body_digest().unwrap(), old.body_digest().unwrap());
                assert_eq!(roundtrip(&new).body_digest().unwrap(), roundtrip(&old).body_digest().unwrap());
            }
        }
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
    fn owned_arrow_source_keeps_nulls_and_complete_identity_on_replay() {
        let memory = budget();
        let batch = RecordBatch::try_from_iter([
            ("_id", Arc::new(arrow_array::StringArray::from(vec!["a", "b"])) as arrow_array::ArrayRef),
            ("payload", Arc::new(arrow_array::BinaryArray::from(vec![Some(b"data".as_slice()), None]))),
        ]).unwrap();
        let batch = OwnedBatch::new(batch, &memory).unwrap();
        let documents = ArrowDocuments::from_owned_payload(&batch, &memory).unwrap().unwrap();
        let source = SourceTransaction::new(vec![TxOp::OwnedArrowDocs {
            relation: rtdb_types::OwnedRelation::new("facts").unwrap(),
            documents, create_only: true, valid_from: None, valid_to: None,
        }], SubmitOptions::default(), &memory).unwrap();
        let replayed = roundtrip(&source);
        let [Operation::OwnedArrowIids { documents, iids, presence: Some(presence), create_only: true, .. }] = replayed.operations.as_slice() else {
            panic!("expected owned Arrow replay");
        };
        assert_eq!(iids, &[EntityId::String("a".into()).iid(), EntityId::String("b".into()).iid()]);
        assert_eq!(documents.array().len(), 2);
        assert!(documents.array().column(1).is_null(1));
        assert!(presence[1].contains("payload"));
    }

    #[test]
    fn binary_owner_source_replays_arrow_and_typed_effects_in_original_order() {
        let memory = budget();
        let relation = rtdb_types::OwnedRelation::new("facts").unwrap();
        let keys: [&[u8]; 2] = [&[0, 255], &[0, 255, 0]];
        let batch = RecordBatch::try_from_iter([
            ("_id", Arc::new(arrow_array::BinaryArray::from_iter_values(keys)) as ArrayRef),
        ]).unwrap();
        let batch = OwnedBatch::new(batch, &memory).unwrap();
        let documents = ArrowDocuments::from_owned_payload(&batch, &memory).unwrap().unwrap();
        let first = EntityId::Binary(keys[0].to_vec());
        let second = EntityId::Binary(keys[1].to_vec());
        let position = rtdb_types::OwnedPointVersion {
            history: Timestamp(8),
            position: rtdb_types::EventPosition { transaction: rtdb_types::TxId(17), ordinal: 3 },
        };
        let source = SourceTransaction::new(vec![
            TxOp::OwnedCheck { relation: relation.clone(), id: first.clone(), valid_at: Timestamp(2), expected: Some(position) },
            TxOp::OwnedArrowDocs { relation: relation.clone(), documents, create_only: false, valid_from: None, valid_to: None },
            TxOp::OwnedDelete { relation, id: second.clone(), valid_from: None, valid_to: None },
        ], SubmitOptions::default(), &memory).unwrap();
        let replay = roundtrip(&source);
        let [
            Operation::Public(TxOp::OwnedCheck { id, expected, .. }),
            Operation::OwnedArrowIids { documents, iids, .. },
            Operation::Public(TxOp::OwnedDelete { id: deleted, .. }),
        ] = replay.operations.as_slice() else { panic!("expected ordered binary effects"); };
        assert_eq!((id, *expected, deleted), (&first, Some(position), &second));
        assert_eq!(documents.owned_id(0).unwrap(), first);
        assert_eq!(documents.owned_id(1).unwrap(), second);
        assert_eq!(iids, &[first.iid(), second.iid()]);
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
    fn arrow_document_ids_preserve_iids_errors_and_source_bytes() {
        let memory = budget();
        let table = TableRef::new("public", "docs");
        let values = [
            Value::I8(i8::MIN), Value::I16(i16::MIN), Value::I32(i32::MIN), Value::I64(i64::MIN),
            Value::Utf8("places/東京".into()), Value::Keyword(rtdb_types::Keyword::from_name("places/東京")),
            Value::Uuid([3; 16]),
        ];
        let rows = |values: &[Value]| values.iter().map(|value| {
            StructFields::from([("_id".into(), value.clone()), ("body".into(), Value::Utf8("document".into()))])
        }).collect::<Vec<_>>();
        let arrow = |values: &[Value]| {
            let logical = LogicalType::join_all(&values.iter().map(Value::logical_type).collect::<Vec<_>>());
            let text = LogicalType::Scalar(rtdb_types::ScalarType::Utf8);
            let fields = vec![to_field("_id", &logical).unwrap(), to_field("body", &text).unwrap()].into();
            let columns = [rtdb_columnar::copy_array(&logical, values).unwrap(),
                rtdb_columnar::copy_array(&text, &vec![Value::Utf8("document".into()); values.len()]).unwrap()];
            let layout = DecodedDocumentLayout::new(&fields, &memory).unwrap();
            let array = rtdb_columnar::build_struct_array(fields, &columns, values.len(), None).unwrap();
            ArrowDocuments::from_source(array.as_struct(), layout, array.get_array_memory_size(), &memory).unwrap().unwrap()
        };
        let put_arrow = |documents| SourceTransaction::new(vec![TxOp::PutArrowDocs {
            table: table.clone(), documents,
        }], SubmitOptions::default(), &memory).unwrap();
        let put_rows = |values: &[Value]| SourceTransaction::new(vec![TxOp::PutDocs {
            table: table.clone(), documents: Documents::new(rows(values), &memory).unwrap(),
            valid_from: None, valid_to: None,
        }], SubmitOptions::default(), &memory).unwrap();

        let batch = arrow(&values);
        let reader = batch.id_reader().unwrap();
        for (row, value) in values.iter().enumerate() {
            assert_eq!(batch.iid(row, reader.as_ref(), &memory).unwrap(), value.entity_id().unwrap().iid());
        }
        let columnar = put_arrow(batch);
        let scalar = put_rows(&values);
        assert_eq!(columnar.encode().unwrap().as_slice(), scalar.encode().unwrap().as_slice());
        assert_eq!(columnar.body_digest().unwrap(), scalar.body_digest().unwrap());

        for bad in [Value::Binary(vec![3; 16]), Value::FixedSizeBinary(vec![3; 16]), Value::DateDay(0), Value::Null] {
            let values = [Value::I64(1), bad, Value::Bool(false)];
            assert_eq!(put_arrow(arrow(&values)).encode().unwrap_err(), put_rows(&values).encode().unwrap_err());
        }
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
            vec![Arc::new(arrow_array::Int64Array::from(vec![2, 3])), Arc::new(arrow_array::BooleanArray::from(vec![true, false]))],
            None,
        ).unwrap();
        let pages = [&first, &second].map(|page| {
            let layout = DecodedDocumentLayout::new(page.fields(), &memory).unwrap();
            ArrowDocuments::from_source(page, layout, page.get_array_memory_size(), &memory).unwrap().unwrap()
        });
        let mut arrays = BatchBuilder::new(4096, &memory).unwrap();

        let result = document_leg(pages.iter().map(SourceDocuments::Arrow).collect(), &mut arrays, &memory).unwrap();

        let lists = result.as_any().downcast_ref::<ListArray>().unwrap();
        assert_eq!(lists.value_offsets(), &[0, 1, 3]);
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
        let list = Value::list(vec![Value::Utf8("list".into())]);
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
        let values = [vec![Value::I64(7), Value::Null], vec![Value::list(vec![Value::Bool(true)])]];
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
        let first = Value::FixedSizeList(vec![Value::U64(u64::MAX), Value::U64(7)].into());
        let second = Value::FixedSizeList(vec![Value::Utf8("a".into()), Value::Utf8("b".into())].into());
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
        assert!(arrow_document_struct(&[first_docs.array(), second_docs.array()], 2, &mut builder).unwrap().is_some());
        let operations = [first_docs, second_docs].into_iter().map(|documents| TxOp::PutArrowDocs {
            table: TableRef::new("public", "docs"), documents,
        }).collect();
        let source = SourceTransaction::new(operations, SubmitOptions::default(), &memory).unwrap();

        let replay = roundtrip(&source);

        assert_eq!(replay.operations.len(), 2);
        for (operation, expected) in replay.operations.iter().zip([Value::FixedSizeList(vec![Value::I64(-1), Value::I64(7)].into()), second]) {
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
            let column = build_array(&list, &[Value::FixedSizeList(vec![Value::I64(value)].into())]).unwrap();
            StructArray::try_new(vec![Arc::clone(&field)].into(), vec![column], None).unwrap()
        };
        let (first, second) = (page(5), page(7));
        let memory = MemoryBudget::new(1 << 18);
        let mut builder = BatchBuilder::new(4096, &memory).unwrap();

        let result = arrow_document_struct(&[&first, &second], 2, &mut builder).unwrap().unwrap();

        let docs = result.as_struct();
        let (field, array) = document_column(docs, "items").unwrap();
        assert_eq!(read_value(field, array.as_ref(), 0).unwrap(), Value::FixedSizeList(vec![Value::I64(5)].into()));
        assert_eq!(read_value(field, array.as_ref(), 1).unwrap(), Value::FixedSizeList(vec![Value::I64(7)].into()));
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
        let right = Value::FixedSizeList(vec![Value::list(vec![Value::Struct(StructFields::from([
            ("n".into(), Value::I64(44)), ("payload".into(), Value::FixedSizeBinary(vec![8; width])),
        ]))])].into());
        let right = build_array(&right.logical_type(), &[right]).unwrap();
        let page = |input: ArrayRef, nullable| {
            let field = Field::new("items", input.data_type().clone(), nullable);
            StructArray::try_new(vec![field].into(), vec![input], None).unwrap()
        };
        let (first, second) = (page(Arc::new(left), true), page(right, false));
        let memory = MemoryBudget::new(2 << 20);
        let mut builder = BatchBuilder::new(4096, &memory).unwrap();

        let result = arrow_document_struct(&[&first, &second], first.len() + second.len(), &mut builder).unwrap().unwrap();

        let docs = result.as_struct();
        let (field, array) = document_column(docs, "items").unwrap();
        let expected = Value::FixedSizeList(vec![Value::list(vec![Value::Struct(StructFields::from([
            ("n".into(), Value::I64(0)), ("payload".into(), Value::Null),
        ]))])].into());
        assert_eq!(read_value(field, array.as_ref(), 0).unwrap(), expected);
        assert_eq!(read_value(field, array.as_ref(), 1).unwrap(), Value::Null);
        assert_eq!(array.as_fixed_size_list().values().as_list::<i32>().value_offsets(), &[0, 1, 1, 2]);
    }

    #[test]
    fn normalizing_nullable_fixed_size_lists_bounds_padding_by_its_null_leg() {
        let width = 1 << 20;
        let element = LogicalType::Scalar(ScalarType::FixedSizeBinary(width)).join(&LogicalType::Scalar(ScalarType::I64)).maybe();
        let list = LogicalType::List { kind: rtdb_types::ListKind::FixedSizeList(1), element: Box::new(element) };
        let value = Value::FixedSizeList(vec![Value::I64(13)].into());
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
            ("items".into(), Value::FixedSizeList(vec![Value::Undefined].into())),
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
            let list = Value::list(vec![Value::Struct(StructFields::from([
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
    fn receipt_float_columns_encode_the_same_ipc_as_the_row_writer() {
        use arrow_array::{Float32Array, Int64Array};

        let memory = MemoryBudget::new(1 << 24);
        let vector = Value::FixedSizeList(vec![Value::F32(f32::from_bits(0x7fc00001)), Value::Null, Value::F32(-0.0)].into());
        let ty = vector.logical_type().maybe();
        let field = to_field("vector", &ty).unwrap();
        let values = copy_array(&ty, &[Value::Null, vector]).unwrap();
        let batch = RecordBatch::try_new(
            Arc::new(Schema::new(vec![Field::new("_id", DataType::Int64, false), field.clone()])),
            vec![Arc::new(Int64Array::from(vec![1, 2])), Arc::clone(&values)],
        ).unwrap();
        let documents = ArrowDocuments::from_record_batch(&batch, &memory).unwrap().unwrap();
        let cells = (0..2).map(|row| DocumentCell::Arrow {
            cell: ArrowCell { field: &field, array: values.as_ref(), row }, declared: Some(&ty),
        }).collect::<Vec<_>>();
        let cells = cells.iter().map(Some).collect::<Vec<_>>();
        let (typed, _) = copy_document_floats(&field, &ty, &cells, &memory).unwrap().unwrap();
        let items = typed.as_fixed_size_list().values().as_any().downcast_ref::<Float32Array>().unwrap();
        assert_eq!(items.value(3).to_bits(), 0x7fc00001);
        assert_eq!(items.value(5).to_bits(), (-0.0_f32).to_bits());
        assert!(items.is_null(4));

        let table = TableRef::new("public", "docs");
        let row = SourceTransaction::from_operations(vec![Operation::Public(TxOp::PutDocs {
            table: table.clone(), documents: documents.to_rows(true, &memory).unwrap(), valid_from: None, valid_to: None,
        })], SubmitOptions::default(), &memory).unwrap();
        let arrow = SourceTransaction::from_operations(vec![Operation::PutReplayArrow {
            table, documents, iids: None, valid_from: None, valid_to: None,
        }], SubmitOptions::default(), &memory).unwrap();
        assert_eq!(arrow.body_digest().unwrap(), row.body_digest().unwrap());
    }

    #[test]
    fn receipt_masks_fixed_list_union_components_in_the_first_leg() {
        let memory = MemoryBudget::new(1 << 24);
        for middle in [Value::F32(1.0), Value::Null] {
            let vector = Value::FixedSizeList(vec![Value::F32(-0.0), middle, Value::F64(f64::from_bits(0x7ff8000000000003))].into());
            let ty = vector.logical_type().maybe();
            let field = to_field("embedding", &ty).unwrap();
            let values = copy_array(&ty, &[Value::Null, vector]).unwrap();
            let input = RecordBatch::try_new(
                Arc::new(Schema::new(vec![Field::new("_id", DataType::Int64, false), field.clone()])),
                vec![Arc::new(arrow_array::Int64Array::from(vec![1, 2])), Arc::clone(&values)],
            ).unwrap();
            let documents = ArrowDocuments::from_record_batch(&input, &memory).unwrap().unwrap();
            let cells = [None, Some(DocumentCell::Arrow {
                cell: ArrowCell { field: &field, array: values.as_ref(), row: 1 }, declared: Some(&ty),
            })];
            let (typed, _) = copy_document_floats(&field, &ty, &cells.each_ref().map(Option::as_ref), &memory).unwrap().unwrap();
            let components = typed.as_fixed_size_list().values().as_union();
            let DataType::Union(legs, _) = components.data_type() else { unreachable!() };
            assert_eq!(components.type_ids()[..3], [legs.iter().next().unwrap().0; 3]);

            let from_rows = |rows: Documents| Operation::Public(TxOp::PutDocs {
                table: TableRef::new("public", "docs"), documents: rows, valid_from: None, valid_to: None,
            });
            let row_documents = documents.to_rows(true, &memory).unwrap();
            let row = SourceTransaction::from_operations(vec![from_rows(row_documents)], SubmitOptions::default(), &memory).unwrap();
            let arrow = SourceTransaction::from_operations(vec![Operation::PutReplayArrow {
                table: TableRef::new("public", "docs"), documents, iids: None, valid_from: None, valid_to: None,
            }], SubmitOptions::default(), &memory).unwrap();
            assert_eq!(arrow.body_digest().unwrap(), row.body_digest().unwrap());
        }
    }

    #[test]
    fn receipt_masks_match_the_row_writer_across_its_chunk_boundary() {
        let memory = MemoryBudget::new(1 << 24);
        let vector = Value::FixedSizeList(vec![Value::F32(-0.0), Value::F64(3.0), Value::Null].into());
        let ty = vector.logical_type().maybe();
        let field = to_field("embedding", &ty).unwrap();
        let values = copy_array(&ty, &[Value::Null, vector]).unwrap();
        let expected = read_value(&field, values.as_ref(), 1).unwrap();
        let schema = Arc::new(Schema::new(vec![field.clone()]));
        let encode = |array| {
            let batch = RecordBatch::try_new(Arc::clone(&schema), vec![array]).unwrap();
            crate::ipc::encode(&schema, [&batch], &memory).unwrap()
        };
        for len in [128, 129] {
            let array = (0..len).map(|row| Some(ArrowCell { field: &field, array: values.as_ref(), row: row % 2 }));
            let (fast, _fast) = copy_float_cells(&field, array, &memory).unwrap();
            let (slow, _slow) = rtdb_columnar::copy_evaluated_cells(&ty, len, &memory, |_| expected.retained_bytes(),
                |row| Ok(if row % 2 == 0 { Value::Null } else { expected.clone() })).unwrap();
            assert_eq!(encode(fast).as_slice(), encode(slow).as_slice());
        }
    }

    #[test]
    fn missing_arrow_id_transit_context_preserves_nullable_float_components() {
        let memory = MemoryBudget::new(1 << 24);
        let document = StructFields::from([
            ("_id".into(), Value::Null),
            ("vector".into(), Value::list(vec![Value::F32(-0.0), Value::Null, Value::F64(f64::INFINITY)])),
            ("label".into(), Value::Utf8("x".into())),
        ]);
        let documents = Documents::new(vec![document.clone()], &memory).unwrap();
        let arrow = owned_row_documents(&documents, &memory).unwrap();
        let expected = document_id(&document, &memory).unwrap_err();
        let direct = arrow.missing_float_document_id(0, &memory).unwrap().unwrap();
        assert_eq!(direct, expected);
        assert_eq!(arrow.iid(0, arrow.id_reader().unwrap().as_ref(), &memory).unwrap_err(), expected);
    }

    #[test]
    fn missing_arrow_id_uses_nested_float_positions_for_transit_context() {
        let memory = MemoryBudget::new(1 << 24);
        let document = StructFields::from([
            ("_id".into(), Value::Null),
            ("details".into(), Value::Struct(StructFields::from([
                ("embedding".into(), Value::list(vec![Value::F32(-0.0), Value::F64(f64::INFINITY), Value::Null])),
                ("label".into(), Value::I32(2)),
                ("empty".into(), Value::Null),
            ]))),
        ]);
        let documents = Documents::new(vec![document.clone()], &memory).unwrap();
        let arrow = owned_row_documents(&documents, &memory).unwrap();
        let expected = document_id(&document, &memory).unwrap_err();
        assert_eq!(arrow.missing_float_document_id(0, &memory).unwrap().unwrap(), expected);
        assert_eq!(arrow.iid(0, arrow.id_reader().unwrap().as_ref(), &memory).unwrap_err(), expected);
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
    fn legacy_owned_vectors_replay_in_arrow_and_keep_source_and_receipt_bytes() {
        let memory = budget();
        let relation = rtdb_types::OwnedRelation::new("vectors").unwrap();
        let rows = vec![
            StructFields::from([
                ("_id".into(), Value::I64(1)),
                ("v".into(), Value::FixedSizeList(rtdb_types::ListValue::from_f32(
                    arrow_array::Float32Array::from(vec![Some(f32::from_bits(0x7fc0_1234)), Some(-0.0), None]),
                ))),
            ]),
            StructFields::from([("_id".into(), Value::I64(2)), ("v".into(), Value::Null)]),
            StructFields::from([("_id".into(), Value::I64(3))]),
            StructFields::from([
                ("_id".into(), Value::I64(4)),
                ("v".into(), Value::list(vec![Value::F64(-0.0), Value::F32(2.0), Value::Null])),
            ]),
            StructFields::from([("_id".into(), Value::I64(5)), ("v".into(), Value::Utf8("scalar".into()))]),
        ];
        let documents = Documents::new(rows, &memory).unwrap();
        let source = SourceTransaction::new(vec![
            TxOp::OwnedInsertDocs { relation: relation.clone(), documents: documents.clone(), valid_from: None, valid_to: None },
            TxOp::OwnedReplaceDocs { relation, documents, valid_from: None, valid_to: None },
        ], SubmitOptions::default(), &memory).unwrap();
        let replay = roundtrip(&source);
        for operation in &replay.operations {
            let Operation::OwnedArrowIids { documents, .. } = operation else { panic!("expected retained Arrow replay"); };
            let Some(LegacyOwnedPresence::Exact(presence)) = documents.legacy_owned_presence() else { panic!("expected exact presence"); };
            assert!(presence.fields[1].contains("v"));
            assert!(!presence.fields[2].contains("v"));
            let (view, range) = ArrowCell {
                field: &documents.array().fields()[1], array: documents.array().column(1).as_ref(), row: 0,
            }.float_list_span().unwrap();
            let rtdb_types::ListView::F32(values) = view else { panic!("expected Float32 components"); };
            assert_eq!(values.value(range.start).to_bits(), 0x7fc0_1234);
            assert_eq!(values.value(range.start + 1).to_bits(), (-0.0f32).to_bits());
            assert!(values.is_null(range.start + 2));
        }
        assert_eq!(source.encode().unwrap().as_slice(), replay.encode().unwrap().as_slice());
        assert_eq!(source.body_digest().unwrap(), replay.body_digest().unwrap());
    }

    #[test]
    fn relative_owned_replay_retains_the_legacy_arrow_payload() {
        let memory = budget();
        let documents = Documents::new(vec![StructFields::from([
            ("_id".into(), Value::I64(1)),
            ("v".into(), Value::list(vec![Value::F64(-0.0), Value::Null, Value::F32(1.0)])),
        ])], &memory).unwrap();
        let source = SourceTransaction::new(vec![TxOp::OwnedRelativePeriod {
            relation: rtdb_types::OwnedRelation::new("vectors").unwrap(),
            effect: OwnedPeriodEffect::Replace(documents),
            period: OwnedRelativePeriod::from_fields(None, Some(Timestamp(100)), Some(Timestamp(-5)), None).unwrap(),
        }], SubmitOptions::default(), &memory).unwrap();
        let mut replay = roundtrip(&source);
        assert!(matches!(&replay.operations[0], Operation::Public(TxOp::OwnedRelativePeriod {
            effect: OwnedPeriodEffect::Arrow { create_only: false, .. }, ..
        })));
        assert_eq!(source.encode().unwrap().as_slice(), replay.encode().unwrap().as_slice());
        assert_eq!(source.body_digest().unwrap(), replay.body_digest().unwrap());
        replay.place_owned_periods(Timestamp(30), true).unwrap();
        assert!(matches!(&replay.operations[0], Operation::Public(TxOp::OwnedArrowDocs {
            create_only: false, valid_from: Some(Timestamp(25)), valid_to: Some(Timestamp(100)), ..
        })));
    }

    #[test]
    fn legacy_owned_replay_preserves_implicit_null_omission() {
        let memory = budget();
        let rows = Documents::new(vec![
            StructFields::from([("_id".into(), Value::I64(1)), ("v".into(), Value::list(vec![Value::F64(-0.0)]))]),
            StructFields::from([("_id".into(), Value::I64(2)), ("v".into(), Value::Null)]),
        ], &memory).unwrap();
        let documents = owned_row_documents(&rows, &memory).unwrap().to_rows(true, &memory).unwrap();
        let source = SourceTransaction::from_operations(vec![Operation::OwnedWriteIids {
            create_only: true, relation: rtdb_types::OwnedRelation::new("vectors").unwrap(), documents,
            iids: [1, 2].map(|id| EntityId::Integer(id).iid()).to_vec(), valid_from: None, valid_to: None,
        }], SubmitOptions::default(), &memory).unwrap();
        let replay = roundtrip(&source);
        let Operation::OwnedArrowIids { documents, .. } = &replay.operations[0] else { panic!("expected retained Arrow replay"); };
        assert!(matches!(documents.legacy_owned_presence(), Some(LegacyOwnedPresence::Implicit)));
        let cell = ArrowCell { field: &documents.array().fields()[1], array: documents.array().column(1).as_ref(), row: 1 };
        assert!(!documents.legacy_owned_cell_is_present(cell).unwrap());
        assert_eq!(source.encode().unwrap().as_slice(), replay.encode().unwrap().as_slice());
        assert_eq!(source.body_digest().unwrap(), replay.body_digest().unwrap());
    }

    #[test]
    fn proposal_landing_frontier_is_recognized_after_arrow_replay() {
        let memory = budget();
        let relation = rtdb_types::OwnedRelation::new(rtdb_types::OWNED_ROOT_PUBLICATION_RELATION).unwrap();
        let id = "public_fact_head:AAAAAAAAAAAAAAAAAAAAAA";
        let documents = Documents::new(vec![StructFields::from([("_id".into(), Value::Utf8(id.into()))])], &memory).unwrap();
        let source = SourceTransaction::new(vec![TxOp::OwnedReplaceDocs {
            relation, documents, valid_from: Some(Timestamp::MIN), valid_to: None,
        }], SubmitOptions::default(), &memory).unwrap();
        let replay = roundtrip(&source);
        let footprint = source.landing_footprint().unwrap();
        for transaction in [&replay, &footprint] {
            assert!(matches!(&transaction.operations[0], Operation::OwnedArrowIids { .. }));
            assert!(crate::hosted_control::proposal_landing_frontier_rows(&transaction.operations[0]).is_some());
        }
        assert_eq!(source.body_digest().unwrap(), replay.body_digest().unwrap());
    }

    #[test]
    fn landing_footprint_reuses_compatible_payloads_and_normalizes_masked_children() {
        use arrow_array::{Int64Array, StringArray};

        let memory = budget();
        let values = [Value::I64(7), Value::Utf8("text".into()), Value::Null];
        let types = values.each_ref().map(Value::logical_type);
        let value_type = LogicalType::join_all(&types);
        let items = to_field("item", &value_type).unwrap();
        for masked_child in [false, true] {
            let field = Field::new("code", DataType::Int64, false);
            let codes = Int64Array::from(vec![Some(10), (!masked_child).then_some(20), Some(30)]);
            let detail = StructArray::new(
                vec![field].into(), vec![Arc::new(codes)], Some(NullBuffer::from(vec![true, false, true])),
            );
            let ids = Arc::new(StringArray::from(vec!["first", "second", "third"]));
            let schema = Arc::new(Schema::new(vec![
                Field::new("_id", DataType::Utf8, false),
                items.clone(),
                Field::new("detail", detail.data_type().clone(), true),
            ]));
            let batch = RecordBatch::try_new(schema, vec![
                ids.clone(), build_array(&value_type, &values).unwrap(), Arc::new(detail),
            ]).unwrap();
            let batch = OwnedBatch::new(batch, &memory).unwrap();
            let documents = ArrowDocuments::from_owned_payload(&batch, &memory).unwrap().unwrap();
            let source = SourceTransaction::new(vec![
                TxOp::OwnedArrowDocs {
                    relation: rtdb_types::OwnedRelation::new("facts").unwrap(), documents, create_only: true,
                    valid_from: Some(Timestamp(10)), valid_to: None,
                },
                TxOp::HostedConstraintRefusal,
            ], SubmitOptions::default(), &memory).unwrap();

            let footprint = source.landing_footprint().unwrap();
            let replay = roundtrip(&source);
            assert_eq!(footprint.encode().unwrap().as_slice(), replay.encode().unwrap().as_slice());
            let [
                Operation::OwnedArrowIids { documents, iids, presence: Some(presence), valid_from: Some(Timestamp(10)), .. },
                Operation::Public(TxOp::HostedConstraintRefusal),
            ] = footprint.operations.as_slice() else { panic!("expected the ordered landing footprint"); };
            assert_eq!(iids[1], EntityId::String("second".into()).iid());
            assert!(presence[2].contains("item"));
            assert!(documents.array().fields()[1].is_nullable());
            let retained_ids = documents.array().column(0).as_any().downcast_ref::<StringArray>().unwrap();
            assert_eq!(retained_ids.value(0).as_ptr(), ids.value(0).as_ptr());
            let detail = documents.array().column(2).as_struct();
            assert!(detail.is_null(1));
            assert!(detail.column(0).is_valid(1));
        }
    }

    #[test]
    fn legacy_flight_vector_upload_encodes_like_row_submission_with_and_without_a_typed_sibling() {
        let memory = budget();
        let relation = rtdb_types::OwnedRelation::new("vectors").unwrap();
        let vector = Value::FixedSizeList(vec![Value::F32(f32::from_bits(0x7fc0_5678)), Value::F64(-0.0), Value::Null].into());
        let fields = vec![Field::new("_id", DataType::Int64, false),
            to_field("v", &vector.logical_type()).unwrap().with_metadata([("description".into(), "vector".into())].into())];
        let upload = |id, fields| {
            let array = build_array(&vector.logical_type(), std::slice::from_ref(&vector)).unwrap();
            let batch = RecordBatch::try_new(Arc::new(Schema::new(fields)),
                vec![Arc::new(arrow_array::Int64Array::from(vec![id])), array]).unwrap();
            OwnedBatch::new(batch, &memory).unwrap()
        };
        let typed = |documents| TxOp::OwnedArrowDocs {
            relation: relation.clone(), documents, create_only: true, valid_from: None, valid_to: None,
        };
        let documents = ArrowDocuments::from_legacy_owned_payload(&upload(1, fields.clone()), &memory).unwrap().unwrap();
        assert!(legacy_page_has_stable_layout(&documents, &BatchBuilder::new(4096, &memory).unwrap()).unwrap());
        let arrow = typed(documents);
        let rows = TxOp::OwnedInsertDocs {
            relation: relation.clone(), documents: Documents::new(vec![StructFields::from([
                ("_id".into(), Value::I64(1)), ("v".into(), vector.clone()),
            ])], &memory).unwrap(), valid_from: None, valid_to: None,
        };
        let sibling = typed(ArrowDocuments::from_owned_payload(&upload(2, vec![fields[0].clone(),
            to_field("v", &vector.logical_type()).unwrap()]), &memory).unwrap().unwrap());
        for sibling in [None, Some(sibling)] {
            let source = |operation| SourceTransaction::new(std::iter::once(operation).chain(sibling.clone()).collect(),
                SubmitOptions::default(), &memory).unwrap();
            let arrow = source(arrow.clone());
            let rows = source(rows.clone());
            assert_eq!(arrow.encode().unwrap().as_slice(), rows.encode().unwrap().as_slice());
            assert_eq!(arrow.body_digest().unwrap(), rows.body_digest().unwrap());
        }
    }

    #[test]
    fn legacy_upload_retags_foreign_union_children_and_retains_struct_companions() {
        use arrow_array::{Float32Array, Float64Array, Int64Array};

        let memory = budget();
        let float = f32::from_bits(0x7fc0_7654);
        let legs = UnionFields::new([17, 3, 29], [
            Field::new("real", DataType::Float32, false),
            Field::new("double", DataType::Float64, false),
            Field::new("missing", DataType::Null, true),
        ]);
        let components = UnionArray::try_new(legs, vec![17, 29, 3].into(), Some(vec![0, 0, 0].into()), vec![
            Arc::new(Float32Array::from(vec![float])), Arc::new(Float64Array::from(vec![-0.0])),
            Arc::new(arrow_array::NullArray::new(1)),
        ]).unwrap();
        let vector = ListArray::new(Arc::new(Field::new("item", components.data_type().clone(), true)),
            OffsetBuffer::new(vec![0, 3].into()), Arc::new(components), None);
        let detail = StructArray::new(vec![Field::new("count", DataType::Int64, false)].into(),
            vec![Arc::new(Int64Array::from(vec![12]))], None);
        let upload = OwnedBatch::new(RecordBatch::try_new(Arc::new(Schema::new(vec![
            Field::new("_id", DataType::Int64, false), Field::new("v", vector.data_type().clone(), false),
            Field::new("detail", detail.data_type().clone(), false),
        ])), vec![Arc::new(Int64Array::from(vec![1])), Arc::new(vector), Arc::new(detail)]).unwrap(), &memory).unwrap();
        let documents = ArrowDocuments::from_legacy_owned_payload(&upload, &memory).unwrap().unwrap();
        let relation = rtdb_types::OwnedRelation::new("vectors").unwrap();
        let source = |operation| SourceTransaction::new(vec![operation], SubmitOptions::default(), &memory).unwrap();
        let rows = source(TxOp::OwnedInsertDocs {
            relation: relation.clone(), documents: Documents::new(vec![StructFields::from([
                ("_id".into(), Value::I64(1)),
                ("v".into(), Value::list(vec![Value::F32(float), Value::Null, Value::F64(-0.0)])),
                ("detail".into(), Value::Struct(StructFields::from([("count".into(), Value::I64(12))]))),
            ])], &memory).unwrap(), valid_from: None, valid_to: None,
        });
        let arrow = source(TxOp::OwnedArrowDocs { relation, documents, create_only: true, valid_from: None, valid_to: None });
        assert_eq!(arrow.encode().unwrap().as_slice(), rows.encode().unwrap().as_slice());
        assert_eq!(arrow.body_digest().unwrap(), rows.body_digest().unwrap());
        assert!(matches!(roundtrip(&rows).operations.as_slice(), [Operation::OwnedArrowIids { .. }]));
    }

    #[test]
    fn legacy_source_preserves_dense_union_padding_beneath_null_companions() {
        let memory = budget();
        let relation = rtdb_types::OwnedRelation::new("companions").unwrap();
        let rows = Documents::new((0..130).map(|id| {
            let detail = match id {
                0 => Value::Struct(StructFields::from([("value".into(), Value::I64(1))])),
                1 => Value::Struct(StructFields::from([("value".into(), Value::Utf8("text".into()))])),
                2 => Value::Struct(StructFields::from([("value".into(), Value::Null)])),
                _ => Value::Null,
            };
            StructFields::from([("_id".into(), Value::I64(id)), ("detail".into(), detail)])
        }).collect(), &memory).unwrap();
        let prepared = owned_row_documents(&rows, &memory).unwrap();
        let field = &prepared.array().fields()[1];
        let detail = prepared.array().column(1).as_ref();
        let logical = prepared.column_type("detail").unwrap();
        let cells = (0..rows.rows().len()).map(|row| DocumentCell::Arrow {
            cell: ArrowCell { field, array: detail, row }, declared: Some(logical),
        }).collect::<Vec<_>>();
        let selected = cells.iter().map(Some).collect::<Vec<_>>();
        assert!(canonical_arrow_document_column(field, &selected, &mut BatchBuilder::new(4096, &memory).unwrap()).unwrap().is_none());
        let upload = OwnedBatch::new(RecordBatch::from(prepared.array().clone()), &memory).unwrap();
        let documents = ArrowDocuments::from_legacy_owned_payload(&upload, &memory).unwrap().unwrap();
        let source = |operation| SourceTransaction::new(vec![operation], SubmitOptions::default(), &memory).unwrap();
        let baseline = source(TxOp::OwnedInsertDocs { relation: relation.clone(), documents: rows, valid_from: None, valid_to: None });
        let typed = source(TxOp::OwnedArrowDocs { relation, documents, create_only: true, valid_from: None, valid_to: None });
        assert_eq!(baseline.encode().unwrap().as_slice(), typed.encode().unwrap().as_slice());
        assert_eq!(baseline.body_digest().unwrap(), typed.body_digest().unwrap());
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
        tx.tx_ops = Some(crate::ipc::encode(&schema, [&legacy], &memory).unwrap().into_bytes());
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
        let array: ArrayRef = Arc::new(arrow_array::NullArray::new(1));
        let field = Field::new("omitted", DataType::Null, false);
        let cell = DocumentCell::Arrow { cell: ArrowCell { field: &field, array: &array, row: 0 }, declared: Some(&LogicalType::Nothing) };
        assert_eq!(cell.bound().unwrap(), 0);
        assert_eq!(*cell.value().unwrap(), Value::Undefined);
    }

    #[test]
    fn receipt_digest_matches_buffered_java_ipc_and_length_prefixes() {
        use sha2::{Digest, Sha256};

        let memory = budget();
        let business_metadata = StructFields::from([("business".into(), Value::Utf8("audit".into()))]);
        let mut receipt_metadata = business_metadata.clone();
        receipt_metadata.insert(BOUNDED_RETRY_METADATA_KEY.into(), Value::Struct(StructFields::from([
            ("nonce".into(), Value::Utf8("0123456789abcdef0123456789abcdef".into())),
            ("not_after_micros".into(), Value::I64(10)),
        ])));
        let source = SourceTransaction::new(vec![TxOp::Sql {
            sql: format!("ASSERT TRUE /*{}*/", "x".repeat(16_384)), args: None,
        }], SubmitOptions {
            default_tz: "Europe/Paris".into(),
            user_metadata: Some(UserMetadata::new(Value::Struct(receipt_metadata), &memory).unwrap()),
            ..Default::default()
        }, &memory).unwrap();

        let batch = source.operation_batch(None, SourcePurpose::Receipt).unwrap();
        let ops = crate::ipc::encode(batch.batch().schema_ref(), [batch.batch()], &memory).unwrap();
        let metadata = UserMetadata::new(Value::Struct(business_metadata), &memory).unwrap();
        let metadata = encode_metadata(&metadata, &memory).unwrap();
        let mut expected = Sha256::new();
        for field in [ops.as_slice(), b"Europe/Paris", metadata.as_slice()] {
            expected.update((field.len() as u64).to_be_bytes());
            expected.update(field);
        }
        assert_eq!(source.body_digest().unwrap(), format!("{:x}", expected.finalize()));
    }

    #[test]
    fn canonical_source_receipts_gather_extended_types_with_legacy_bytes() {
        use rtdb_types::{DecimalValue, RunEndWidth};

        let memory = budget();
        let decimal = Value::Decimal(DecimalValue::new((-913i128).to_be_bytes().to_vec(), 64, 129, 128).unwrap());
        let interval = Value::IntervalMonthDayMicro { months: -7, days: 5, nanoseconds: i64::MIN + 1 };
        let keyword = Value::Keyword(rtdb_types::Keyword::from_name(":stored/name"));
        let uri = Value::Uri("https://example.test/a%2Fb?q=1".into());
        let map = Value::Map { sorted: false, entries: vec![
            (keyword.clone(), interval.clone()), (keyword.clone(), Value::Null),
        ] };
        let run = LogicalType::Scalar(ScalarType::I32).run_end_encoded(RunEndWidth::I16).unwrap();
        let values = [
            ("decimal", decimal), ("interval", interval), ("map", map), ("keyword", keyword), ("uri", uri),
            ("float_bits", Value::F64(f64::from_bits(0xfff8_0000_0000_0009))),
            ("year_month", Value::IntervalYearMonth(-7)),
            ("day_time", Value::IntervalDayTime { days: 2, milliseconds: -300 }),
            ("month_day_nano", Value::IntervalMonthDayNano { months: -1, days: 2, nanoseconds: -3 }),
        ];

        for length in [5, 130] {
            let mut fields = vec![to_field("_id", &LogicalType::Scalar(ScalarType::I32)).unwrap(), to_field("run", &run).unwrap()];
            let mut columns = vec![
                Arc::new(arrow_array::Int32Array::from_iter_values(0..=length as i32)) as ArrayRef,
                copy_array(&run, &(0..=length).map(|row| if row % 5 < 3 { Value::I32(-5) } else { Value::I32(7) }).collect::<Vec<_>>()).unwrap(),
            ];
            for (name, value) in &values {
                let ty = value.logical_type().maybe();
                fields.push(to_field(*name, &ty).unwrap());
                let values = (0..=length).map(|row| if row % 3 == 0 { Value::Null } else { value.clone() }).collect::<Vec<_>>();
                columns.push(copy_array(&ty, &values).unwrap());
            }
            let array = StructArray::try_new(fields.into(), columns, None).unwrap().slice(1, length);
            let layout = DecodedDocumentLayout::new(array.fields(), &memory).unwrap();
            let arrow = ArrowDocuments::from_source(&array, layout, array.get_array_memory_size(), &memory).unwrap().unwrap();
            let rows = arrow.to_rows(false, &memory).unwrap();
            assert!(arrow.requires_legacy_receipt(true));
            for (field, column) in array.fields().iter().zip(array.columns()).skip(1) {
                let logical = arrow.column_type(field.name()).unwrap();
                let ty = logical.unencoded();
                let target = to_field(field.name(), &ty).unwrap();
                let mut cells = vec![DocumentCell::Value(Cow::Borrowed(&rows.rows()[0][field.name()]))];
                cells.extend((1..length).map(|row| DocumentCell::Arrow {
                    cell: ArrowCell { field, array: column.as_ref(), row },
                    declared: Some(logical),
                }));
                let selected = cells.iter().map(Some).collect::<Vec<_>>();
                assert!(gather_document_column(&target, &ty, &selected, &memory).is_some(), "{}", field.name());
            }
            let iids = (1..=length).map(|id| EntityId::Integer(id as i64).iid()).collect::<Vec<_>>();
            let table = TableRef::new("public", "docs");
            let actual = SourceTransaction::from_operations(vec![Operation::PatchArrowIids {
                table: table.clone(), documents: arrow, iids: iids.clone(), valid_from: None, valid_to: None,
            }], SubmitOptions::default(), &memory).unwrap();
            let expected = SourceTransaction::from_operations(vec![Operation::PatchIids {
                table, documents: rows, iids, valid_from: None, valid_to: None,
            }], SubmitOptions::default(), &memory).unwrap();
            let receipt_ipc = |source: &SourceTransaction| {
                let batch = source.operation_batch(None, SourcePurpose::Receipt).unwrap();
                crate::ipc::encode(batch.batch().schema_ref(), [batch.batch()], &memory).unwrap()
            };
            assert_eq!(receipt_ipc(&actual).as_slice(), receipt_ipc(&expected).as_slice());
            assert_eq!(actual.body_digest().unwrap(), expected.body_digest().unwrap());
            assert_eq!(roundtrip(&actual).body_digest().unwrap(), expected.body_digest().unwrap());
        }
    }

    #[test]
    fn canonical_source_coalescing_keeps_legacy_aliases_and_packed_null_bytes() {
        use arrow_array::{Int64Array, StringArray};
        use arrow_buffer::NullBuffer;

        let memory = budget();
        let uri = Value::Uri("https://example.test/retained".into());
        let uri_type = uri.logical_type();
        let alias = UnionArray::try_new(UnionFields::try_new([3, 4, 6], [
            Field::new("first", DataType::Int64, false),
            Field::new("second", DataType::Int64, false),
            Field::new("utf8", DataType::Utf8, false),
        ]).unwrap(), vec![4i8].into(), Some(vec![0i32].into()), vec![
            Arc::new(Int64Array::from(Vec::<i64>::new())),
            Arc::new(Int64Array::from(vec![7])),
            Arc::new(StringArray::from(Vec::<&str>::new())),
        ]).unwrap();
        let numbers = Arc::new(Int64Array::new(vec![91, 5].into(), Some(NullBuffer::from(vec![false, true]))));
        let numbers_type = LogicalType::list(LogicalType::Scalar(ScalarType::I64).maybe());
        let numbers_field = to_field("numbers", &numbers_type).unwrap();
        let DataType::List(item) = numbers_field.data_type() else { unreachable!() };
        let numbers = ListArray::new(Arc::clone(item), OffsetBuffer::new(vec![0, 2].into()), numbers, None);
        let fields = vec![
            Field::new("alias", alias.data_type().clone(), false),
            numbers_field,
            to_field("uri", &uri_type).unwrap(),
        ];
        let array = StructArray::new(fields.into(), vec![
            Arc::new(alias), Arc::new(numbers), copy_array(&uri_type, &[uri]).unwrap(),
        ], None);
        let layout = DecodedDocumentLayout::new(array.fields(), &memory).unwrap();
        let documents = ArrowDocuments::from_source(&array, layout, array.get_array_memory_size(), &memory).unwrap().unwrap();
        let legacy = documents.to_rows(false, &memory).unwrap();
        let row = SourceDocuments::Rows(&legacy);
        let mut builder = BatchBuilder::new(4096, &memory).unwrap();
        assert!(mixed_document_struct(&[SourceDocuments::Arrow(&documents), SourceDocuments::Rows(&legacy)], 2, &mut builder).unwrap().is_none());
        let actual = document_leg(vec![SourceDocuments::Arrow(&documents), row], &mut builder, &memory).unwrap();
        let expected = document_leg(vec![SourceDocuments::Rows(&legacy), SourceDocuments::Rows(&legacy)], &mut builder, &memory).unwrap();
        let ipc = |array: ArrayRef| {
            let field = Field::new("docs", array.data_type().clone(), false);
            let batch = RecordBatch::try_new(Arc::new(Schema::new(vec![field])), vec![array]).unwrap();
            crate::ipc::encode(batch.schema_ref(), [&batch], &memory).unwrap()
        };
        assert_eq!(ipc(actual).as_slice(), ipc(expected).as_slice());
        assert!(!canonical::selectable(&array.fields()[0]));
        assert!(!canonical::selectable_cell(ArrowCell { field: &array.fields()[1], array: array.column(1).as_ref(), row: 0 }, true));
    }

    #[test]
    fn canonical_source_receipts_keep_non_float_union_padding_at_the_chunk_boundary() {
        let memory = budget();
        let ty = LogicalType::List {
            kind: ListKind::FixedSizeList(2),
            element: Box::new(LogicalType::Scalar(ScalarType::Utf8).join(&LogicalType::Scalar(ScalarType::I64)).maybe()),
        }.maybe();
        let field = to_field("items", &ty).unwrap();
        assert!(!canonical::selectable(&field));
        for length in [128, 129] {
            let values = (0..length).map(|row| if row % 3 == 0 { Value::Null } else {
                Value::FixedSizeList(vec![Value::Utf8("selected".into()), Value::I64(row as i64)].into())
            }).collect::<Vec<_>>();
            let ids = to_field("_id", &LogicalType::Scalar(ScalarType::I64)).unwrap();
            let array = StructArray::new(vec![ids, field.clone()].into(), vec![
                Arc::new(arrow_array::Int64Array::from_iter_values(0..length as i64)), copy_array(&ty, &values).unwrap(),
            ], None);
            let layout = DecodedDocumentLayout::new(array.fields(), &memory).unwrap();
            let arrow = ArrowDocuments::from_source(&array, layout, array.get_array_memory_size(), &memory).unwrap().unwrap();
            let rows = arrow.to_rows(false, &memory).unwrap();
            let iids = (0..length).map(|row| EntityId::Integer(row as i64).iid()).collect::<Vec<_>>();
            let table = TableRef::new("public", "docs");
            let actual = SourceTransaction::from_operations(vec![Operation::PatchArrowIids {
                table: table.clone(), documents: arrow, iids: iids.clone(), valid_from: None, valid_to: None,
            }], SubmitOptions::default(), &memory).unwrap();
            let expected = SourceTransaction::from_operations(vec![Operation::PatchIids {
                table, documents: rows, iids, valid_from: None, valid_to: None,
            }], SubmitOptions::default(), &memory).unwrap();
            assert_eq!(actual.body_digest().unwrap(), expected.body_digest().unwrap());
        }
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
        tx.user_metadata = Some(Bytes::new());
        assert!(SourceTransaction::decode(&message, &memory).unwrap().options.user_metadata.is_none());
    }
}


fn capacity_condition_bytes(relation: &rtdb_types::OwnedRelation, column_bytes: usize, prefix_bytes: usize) -> Result<usize> {
    relation.name().len().checked_add(column_bytes)
        .and_then(|size| size.checked_add(prefix_bytes)).ok_or_else(overflow)
}

fn range_evidence_bytes(relation: &rtdb_types::OwnedRelation, region: &Option<Arc<rtdb_types::OwnedRangeEvidence>>) -> usize {
    relation.name().len() + region.as_ref().map_or(0, |region| region.retained_bytes())
}
