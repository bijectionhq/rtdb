use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use rtdb_columnar::{MemoryBudget, MemoryReservation};
use rtdb_format::basis::{decode_tx_basis, encode_tx_basis};
use rtdb_plan::AccessMode;
use rtdb_tx::{SubmitOptions, SubmittedTx, TransactionResult, TxOp, UserMetadata};
use rtdb_types::{Anomaly, Instant, Result, TimeBasis, Timestamp, TransactionKey, TxBasis, TxId, Value};
use tokio_util::sync::CancellationToken;

use crate::placed_receipts::is_placed_token;

#[async_trait]
pub trait SessionService: Send + Sync {
    fn memory(&self) -> &Arc<MemoryBudget>;
    fn execution_observer(&self) -> Option<Arc<dyn rtdb_plan::ExecutionObserver>> { None }
    fn user(&self) -> Option<&str>;
    fn check_open(&self) -> Result<()>;
    async fn revalidate(&self) -> Result<()>;
    fn now(&self) -> Timestamp;
    /// Supplies the query clock; implementations with finer precision override the microsecond clock lift.
    fn query_instant(&self) -> Instant { self.now().into() }
    async fn fresh_query_instant(&self) -> Result<Instant> { Ok(self.query_instant()) }
    fn snapshot_basis(&self) -> Result<TimeBasis>;
    async fn fresh_snapshot_basis(&self, _cancellation: &CancellationToken) -> Result<(TimeBasis, Option<ReplicaQueryCapture>)> {
        Ok((self.snapshot_basis()?, None))
    }
    async fn await_basis(&self, basis: Option<TxBasis>, timeout: Duration, cancellation: &CancellationToken) -> Result<()>;
    async fn await_client_token(&self, token: Option<&str>, timeout: Duration, cancellation: &CancellationToken) -> Result<Option<String>> {
        self.await_basis(token.map(decode_tx_basis).transpose()?, timeout, cancellation).await?;
        Ok(None)
    }
    async fn check_submission_basis(&self, _database: &str, _token: Option<&str>, _cancellation: &CancellationToken) -> Result<()> { Ok(()) }
    async fn submit(&self, database: &str, operations: Vec<TxOp>, options: SubmitOptions, cancellation: &CancellationToken) -> Result<SubmittedTx>;
    async fn submit_replay(&self, _database: &str, _operations: Vec<TxOp>, _options: SubmitOptions, _permit: ReplaySubmissionPermit, _cancellation: &CancellationToken) -> Result<SubmittedTx> {
        Err(Anomaly::unsupported("rtdb.node/private-replay-unsupported", "This session service cannot submit a private replay"))
    }
    async fn await_transaction(&self, database: &str, tx_id: TxId, cancellation: &CancellationToken) -> Result<TransactionResult>;
    async fn await_submission(&self, database: &str, submitted: &SubmittedTx, cancellation: &CancellationToken) -> Result<(TransactionResult, Option<String>)> {
        Ok((self.await_transaction(database, submitted.tx_id, cancellation).await?, None))
    }
}

#[derive(Clone, Debug)]
pub struct ReplicaQueryCapture {
    pub(crate) attachment: std::sync::Weak<crate::Database>,
    pub(crate) snapshot: Arc<rtdb_index::Snapshot>,
}

impl PartialEq for ReplicaQueryCapture {
    fn eq(&self, other: &Self) -> bool {
        self.attachment.ptr_eq(&other.attachment) && Arc::ptr_eq(&self.snapshot, &other.snapshot)
    }
}

impl Eq for ReplicaQueryCapture {}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct QueryBasis {
    pub snapshot: TimeBasis,
    pub replica_capture: Option<ReplicaQueryCapture>,
    pub resume_token: Option<String>,
    pub has_await_floor: bool,
    pub snapshot_time: Option<Timestamp>,
    pub current_time: Instant,
    pub timezone: String,
}

#[derive(Clone, Debug, Default)]
pub enum OptionUpdate<T> { #[default] Unchanged, Clear, Set(T) }
impl<T: Clone> OptionUpdate<T> {
    fn apply(&self, old: &Option<T>) -> Option<T> {
        match self { Self::Unchanged => old.clone(), Self::Clear => None, Self::Set(value) => Some(value.clone()) }
    }
    fn is_unchanged(&self) -> bool { matches!(self, Self::Unchanged) }
}

#[derive(Clone, Debug, Default)]
pub struct ReadOnlySettings {
    pub snapshot_token: OptionUpdate<String>,
    pub snapshot_time: OptionUpdate<Timestamp>,
    pub clock_time: OptionUpdate<Instant>,
    pub await_token: OptionUpdate<String>,
}

#[derive(Clone, Debug, Default)]
pub struct ReadWriteSettings {
    pub system_time: OptionUpdate<Timestamp>,
    pub user_metadata: OptionUpdate<UserMetadata>,
    pub asynchronous: OptionUpdate<bool>,
}

#[derive(Clone, Debug)]
pub enum AccessSettings { ReadOnly(ReadOnlySettings), ReadWrite(ReadWriteSettings) }

#[derive(Clone, Debug, Default)]
pub struct TransactionSettings {
    pub access: Option<AccessSettings>,
    pub timezone: OptionUpdate<String>,
}

/// A trusted frontend's one-command key and exclusive submission cutoff in UTC microseconds.
/// The key is 32 hexadecimal digits followed by `/0`.
/// The frontend supplies the same permit on the initial submission and each retry.
pub struct ReplaySubmissionPermit {
    pub key: String,
    pub not_after: Timestamp,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum CommitMode { Sync, Async }

enum Submission {
    Ordinary,
    Replay(ReplaySubmissionPermit),
    PresentReservation,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum TransactionStatus { Idle, Pending, Reading, Writing, Failed, Closed }

#[derive(Clone, Debug)]
pub enum CommitResult { NoTransaction, Submitted(SubmittedTx), Executed(TransactionResult) }

#[derive(Clone, Debug)]
pub enum LastSubmittedTx { Submitted(SubmittedTx), Executed(TransactionResult) }
impl LastSubmittedTx {
    pub fn tx_id(&self) -> TxId { match self { Self::Submitted(tx) => tx.tx_id, Self::Executed(tx) => tx.tx_key().tx_id } }
}

#[derive(Clone, Debug, Default)]
pub struct AutonomousTxOptions {
    pub database: Option<String>,
    pub default_timezone: Option<String>,
    pub system_time: Option<Timestamp>,
    pub user_metadata: Option<UserMetadata>,
}

#[derive(Clone, Debug, Default)]
struct ReadOptions {
    snapshot_token: Option<RetainedText>,
    snapshot_time: Option<Timestamp>,
    clock_time: Option<Instant>,
    await_token: Option<RetainedText>,
}

#[derive(Clone, Debug, Default)]
struct WriteOptions {
    system_time: Option<Timestamp>,
    user_metadata: Option<UserMetadata>,
    asynchronous: bool,
}

#[derive(Clone, Debug)]
struct RetainedText { value: Arc<String>, _reservation: Arc<MemoryReservation> }
impl RetainedText {
    fn new(value: String, memory: &Arc<MemoryBudget>) -> Result<Self> {
        let reservation = memory.reserve(value.capacity().checked_add(128).ok_or_else(allocation_overflow)?)?;
        Ok(Self { value: Arc::new(value), _reservation: reservation })
    }
}
impl std::ops::Deref for RetainedText { type Target = str; fn deref(&self) -> &str { &self.value } }

struct Parameter { value: Option<RetainedText>, _entry: Arc<MemoryReservation> }
struct BufferedOperation { operation: TxOp, _reservation: Arc<MemoryReservation> }
#[derive(Default)]
struct DmlBuffer { operations: Vec<BufferedOperation> }
impl DmlBuffer {
    fn append(&mut self, operation: TxOp, memory: &Arc<MemoryBudget>) -> Result<()> {
        let bytes = match &operation {
            TxOp::OwnedInsertDocs { relation, .. } | TxOp::OwnedReplaceDocs { relation, .. } | TxOp::OwnedArrowDocs { relation, .. } => relation.name().len(),
            TxOp::OwnedRelativePeriod { relation, effect, .. } => {
                let identity = match effect {
                    rtdb_tx::OwnedPeriodEffect::Delete(id) => id.as_value().retained_bytes()?,
                    rtdb_tx::OwnedPeriodEffect::Create(_) | rtdb_tx::OwnedPeriodEffect::Replace(_) | rtdb_tx::OwnedPeriodEffect::Arrow { .. } => 0,
                };
                relation.name().len().checked_add(identity).ok_or_else(allocation_overflow)?
            },
            TxOp::OwnedCheck { relation, id, .. } | TxOp::OwnedDelete { relation, id, .. } | TxOp::OwnedErase { relation, id } | TxOp::OwnedReplayResidue { relation, id, .. } | TxOp::OwnedSweepExpired { relation, id, .. } => relation.name().len()
                .checked_add(id.as_value().retained_bytes()?).ok_or_else(allocation_overflow)?,
            TxOp::OwnedDeclareSearchPath { relation, name, column, .. } | TxOp::OwnedDeclareUniquePath { relation, name, column }
                | TxOp::OwnedDeclareVectorPath { relation, name, column, .. } =>
                (relation.name().len() + name.name().len()).checked_add(column.capacity()).ok_or_else(allocation_overflow)?,
            TxOp::OwnedCheckCapacity { relation, column, prefix, .. } => relation.name().len()
                .checked_add(column.capacity()).and_then(|bytes| bytes.checked_add(prefix.capacity()))
                .ok_or_else(allocation_overflow)?,
            TxOp::OwnedDeclareCapacity { relation, column, when, .. } =>
                (relation.name().len() + column.capacity()).checked_add(when.capacity()).ok_or_else(allocation_overflow)?,
            TxOp::OwnedDeclareRelation { relation, schema } => relation.name().len().checked_add(schema.retained_bytes()).ok_or_else(allocation_overflow)?,
            TxOp::OwnedDropSpatialPath { relation, name } => relation.name().len() + name.name().len(),
            TxOp::OwnedRetireRelation { relation, .. } => relation.name().len(),
            TxOp::OwnedAdoptRelation { source, destination, .. } => source.name().len() + destination.name().len(),
            TxOp::OwnedDeclarePath { relation, name, columns } | TxOp::OwnedDeclareSpatialPath { relation, name, columns } | TxOp::OwnedDeclareEncodedSpatialPath { relation, name, columns, .. } | TxOp::OwnedRequireSpatialPath { relation, name, columns, .. } => columns.iter().try_fold(
                relation.name().len() + name.name().len(), |bytes, column| bytes.checked_add(column.capacity() + 64).ok_or_else(allocation_overflow),
            )?,
            TxOp::HostedControl(control) => control.retained_bytes()?,
            TxOp::OwnedForkWorld { record, attempt, .. } => Value::document_retained_bytes(record)?.saturating_add(attempt.len() + 128),
            TxOp::OwnedPublishRoot { attempt, .. } | TxOp::OwnedPublishWorld { attempt, .. } => attempt.len() + 128,
            TxOp::OwnedCheckWorldPurge { relation, .. } => relation.name().len(),
            TxOp::OwnedDeadline { .. } | TxOp::OwnedClockSlot { .. } | TxOp::OwnedCheckWorldScope { .. } | TxOp::HostedConstraintRefusal => 0,
            TxOp::OwnedDeclareFixedRange { relation, rule } => relation.name().len() + rule.path.capacity() + rule.column.capacity(),
            TxOp::OwnedCheckRange { relation, region, .. } =>
                relation.name().len() + region.as_ref().map_or(0, |region| region.retained_bytes()),
            TxOp::Sql { sql, .. } | TxOp::SqlBytes { sql, .. } => sql.capacity(),
            TxOp::InsertDocs { table, .. } | TxOp::PutDocs { table, .. } | TxOp::PutArrowDocs { table, .. } | TxOp::WriteArrowDocs { table, .. } | TxOp::PatchDocs { table, .. } | TxOp::DeleteDocs { table, .. } | TxOp::EraseDocs { table, .. } => table.schema.capacity().checked_add(table.table.capacity()).ok_or_else(allocation_overflow)?,
        }.checked_add(std::mem::size_of::<BufferedOperation>() * 2 + 128).ok_or_else(allocation_overflow)?;
        let reservation = memory.reserve(bytes)?;
        match (self.operations.last_mut().map(|entry| &mut entry.operation), operation) {
            (Some(TxOp::Sql { sql: previous, args: Some(target) }), TxOp::Sql { sql, args: Some(args) }) if *previous == sql => target.append(args),
            (_, operation) => { self.operations.push(BufferedOperation { operation, _reservation: reservation }); Ok(()) }
        }
    }
    fn is_empty(&self) -> bool { self.operations.is_empty() }
}

enum Phase {
    Pending(Option<ReadOptions>),
    Reading { basis: RetainedBasis, await_token: Option<RetainedText> },
    Writing { buffer: DmlBuffer, options: WriteOptions },
}

struct Transaction {
    phase: Phase,
    failed: bool,
    timezone: RetainedText,
    session_timezone: RetainedText,
}
impl Transaction {
    fn new(phase: Phase, timezone: &RetainedText) -> Self { Self { phase, failed: false, timezone: timezone.clone(), session_timezone: timezone.clone() } }
    fn await_token(&self) -> Option<&str> {
        match &self.phase {
            Phase::Pending(Some(options)) => options.await_token.as_deref(),
            Phase::Reading { await_token, .. } => await_token.as_deref(),
            _ => None,
        }
    }
    fn can_shape(&self) -> bool {
        matches!(&self.phase, Phase::Pending(_)) || matches!(&self.phase, Phase::Writing { buffer, .. } if buffer.is_empty())
    }
}

enum Lifecycle { Open(Option<Transaction>), Closed }

struct RetainedBasis { basis: QueryBasis, _reservation: Arc<MemoryReservation> }
impl RetainedBasis {
    fn copy(basis: &QueryBasis, memory: &Arc<MemoryBudget>) -> Result<Self> {
        let mut bytes = basis.timezone.capacity().checked_add(256)
            .and_then(|bytes| bytes.checked_add(basis.resume_token.as_ref().map_or(0, String::capacity)))
            .ok_or_else(allocation_overflow)?;
        for (name, values) in &basis.snapshot.databases {
            bytes = bytes.checked_add(name.capacity()).and_then(|bytes| bytes.checked_add(values.capacity().checked_mul(std::mem::size_of::<Option<rtdb_types::Instant>>())?)).and_then(|bytes| bytes.checked_add(256)).ok_or_else(allocation_overflow)?;
        }
        let reservation = memory.reserve(bytes)?;
        Ok(Self { basis: basis.clone(), _reservation: reservation })
    }
}

#[derive(serde::Serialize, serde::Deserialize)]
struct GatewaySettings {
    parameters: BTreeMap<String, Option<String>>,
    timezone: String,
    access_mode: Option<AccessMode>,
}

fn portable_parameter(name: &str) -> bool {
    !["user", "database", "options", "rtdb.read", "rtdb.capability", "rtdb.delegable",
        "rtdb.gateway_await_token", "rtdb.gateway_settings", crate::node::IDEMPOTENCY_KEY_PARAMETER]
        .iter().any(|reserved| name.eq_ignore_ascii_case(reserved))
}

fn world_clock(value: &str) -> Result<Instant> {
    let micros = value.parse::<i64>().map(Timestamp)
        .map_err(|_| Anomaly::incorrect("rtdb/invalid-world-clock", "Invalid bijection.at microsecond timestamp"))?;
    micros.next()?;
    Ok(micros.into())
}

pub struct Session {
    service: Arc<dyn SessionService>,
    database: RetainedText,
    lifecycle: Lifecycle,
    auto_commit: bool,
    default_access_mode: Option<AccessMode>,
    timezone: RetainedText,
    initial_timezone: RetainedText,
    await_token: Option<RetainedText>,
    await_token_revision: u64,
    settings_revision: u64,
    /// Advances whenever a parameter is set, restored, reset or cleared.
    parameters_revision: u64,
    last_submitted: Option<LastSubmittedTx>,
    parameters: BTreeMap<String, Parameter>,
    gateway_key: Option<(RetainedText, u64)>,
    /// The session key its transactions were last keyed under, and the sequence number of the next.
    idempotency_sequence: Option<(String, u64)>,
    cancellation: CancellationToken,
}

impl Session {
    pub fn new(service: Arc<dyn SessionService>, database: impl Into<String>, timezone: impl Into<String>, parent: &CancellationToken) -> Result<Self> {
        let timezone = rtdb_expr::normalize_timezone(&timezone.into())?;
        service.check_open()?;
        let database = RetainedText::new(database.into(), service.memory())?;
        let timezone = RetainedText::new(timezone, service.memory())?;
        Ok(Self { service, database, lifecycle: Lifecycle::Open(None), auto_commit: true, default_access_mode: None,
            initial_timezone: timezone.clone(), timezone, await_token: None, await_token_revision: 0, settings_revision: 0, parameters_revision: 0, last_submitted: None, parameters: BTreeMap::new(), gateway_key: None, idempotency_sequence: None, cancellation: parent.child_token() })
    }
    fn ensure_open(&self) -> Result<()> {
        if matches!(self.lifecycle, Lifecycle::Closed) { return Err(Anomaly::incorrect("xtdb/connection-closed", "Connection is closed")); }
        if self.cancellation.is_cancelled() { return Err(Anomaly::new(rtdb_types::AnomalyCategory::Interrupted, "xtdb/connection-cancelled", "Connection was cancelled")); }
        self.service.check_open()
    }
    /// Refreshes shared credentials on the caller's task before a protocol command.
    pub async fn revalidate(&self) -> Result<()> {
        self.ensure_open()?;
        self.cancellation.run_until_cancelled(self.service.revalidate()).await
            .ok_or_else(|| Anomaly::new(rtdb_types::AnomalyCategory::Interrupted, "xtdb/connection-cancelled", "Connection was cancelled"))?
    }
    fn transaction(&self) -> Option<&Transaction> { match &self.lifecycle { Lifecycle::Open(tx) => tx.as_ref(), Lifecycle::Closed => None } }
    fn transaction_mut(&mut self) -> Option<&mut Transaction> { match &mut self.lifecycle { Lifecycle::Open(tx) => tx.as_mut(), Lifecycle::Closed => None } }
    fn require_usable_transaction(&self) -> Result<()> {
        self.ensure_open()?;
        if self.transaction().is_some_and(|tx| tx.failed) { return Err(Anomaly::incorrect("xtdb/tx-failed", "current transaction is aborted, commands ignored until ROLLBACK is received")); }
        Ok(())
    }
    pub fn database(&self) -> &str { &self.database }
    pub fn set_database(&mut self, database: impl Into<String>) -> Result<()> { self.ensure_open()?; self.database = RetainedText::new(database.into(), self.service.memory())?; Ok(()) }
    pub fn application_name(&self) -> &str { self.parameter("application_name").unwrap_or("XTDB") }
    pub fn set_application_name(&mut self, name: impl Into<String>) -> Result<()> { self.set_parameter("application_name", Some(name.into())) }
    pub fn user(&self) -> Option<&str> { self.service.user() }
    pub fn auto_commit(&self) -> bool { self.auto_commit }
    pub fn effective_timezone(&self) -> &str { self.transaction().map_or(&self.timezone, |tx| &tx.timezone) }
    pub fn session_timezone(&self) -> &str { self.transaction().map_or(&self.timezone, |tx| &tx.session_timezone) }
    pub fn effective_await_token(&self) -> Option<&str> { self.transaction().and_then(Transaction::await_token).or(self.await_token.as_deref()) }
    pub fn connection_await_token(&self) -> (u64, Option<&str>) { (self.await_token_revision, self.await_token.as_deref()) }
    pub fn last_submitted(&self) -> Option<&LastSubmittedTx> { self.last_submitted.as_ref() }
    pub fn last_submitted_await_token(&self) -> Option<&str> {
        if let Some(LastSubmittedTx::Submitted(tx)) = self.last_submitted()
            && let Some(receipt) = &tx.placed_receipt
        {
            return Some(receipt);
        }
        self.effective_await_token()
    }
    pub fn cancellation(&self) -> &CancellationToken { &self.cancellation }
    pub fn status(&self) -> TransactionStatus {
        match &self.lifecycle {
            Lifecycle::Closed => TransactionStatus::Closed,
            Lifecycle::Open(None) => TransactionStatus::Idle,
            Lifecycle::Open(Some(tx)) if tx.failed => TransactionStatus::Failed,
            Lifecycle::Open(Some(tx)) => match &tx.phase { Phase::Pending(_) => TransactionStatus::Pending, Phase::Reading { .. } => TransactionStatus::Reading, Phase::Writing { .. } => TransactionStatus::Writing },
        }
    }
    pub fn is_read_only(&self) -> bool { matches!(self.transaction().map(|tx| &tx.phase), Some(Phase::Reading { .. } | Phase::Pending(Some(_)))) }
    pub fn is_read_write(&self) -> bool { matches!(self.transaction().map(|tx| &tx.phase), Some(Phase::Writing { .. })) }
    pub fn set_await_token(&mut self, token: Option<String>) -> Result<()> {
        self.ensure_open()?;
        let token = token.map(|token| RetainedText::new(token, self.service.memory())).transpose()?;
        self.replace_await_token(token);
        Ok(())
    }
    fn replace_await_token(&mut self, token: Option<RetainedText>) {
        if self.await_token.as_deref() != token.as_deref() {
            self.await_token = token;
            self.await_token_revision = self.await_token_revision.wrapping_add(1);
        }
    }
    pub fn set_session_characteristics(&mut self, access: Option<AccessMode>) -> Result<()> {
        self.ensure_open()?;
        if self.default_access_mode != access {
            self.default_access_mode = access;
            self.settings_revision = self.settings_revision.wrapping_add(1);
        }
        Ok(())
    }
    pub fn set_parameter(&mut self, name: impl Into<String>, value: Option<String>) -> Result<()> {
        self.ensure_open()?;
        let name = name.into();
        if name == "bijection.at" {
            if self.transaction().is_some() {
                return Err(Anomaly::incorrect("rtdb/world-setting-in-transaction", "SET bijection.at requires an idle session"));
            }
            if let Some(value) = value.as_deref() { world_clock(value)?; }
        }
        let value = value.map(|value| RetainedText::new(value, self.service.memory())).transpose()?;
        let entry = self.service.memory().reserve(name.capacity().checked_add(256).ok_or_else(allocation_overflow)?)?;
        if portable_parameter(&name) {
            self.settings_revision = self.settings_revision.wrapping_add(1);
        }
        self.parameters.insert(name, Parameter { value, _entry: entry });
        self.parameters_revision = self.parameters_revision.wrapping_add(1);
        Ok(())
    }
    pub fn set_gateway_key(&mut self, key: String) -> Result<()> {
        self.ensure_open()?;
        self.gateway_key = Some((RetainedText::new(key, self.service.memory())?, 0));
        Ok(())
    }
    pub fn settings_revision(&self) -> u64 { self.settings_revision }
    /// Changes whenever `parameter_entries` might; the effective timezone changes independently of it.
    pub fn parameters_revision(&self) -> u64 { self.parameters_revision }

    /// Encodes the mutable settings of the idle session for restoration after a gateway reconnect.
    pub fn encode_gateway_settings(&self) -> Result<(Vec<u8>, Arc<MemoryReservation>)> {
        let entries = self.parameter_entries().filter(|(name, _)| portable_parameter(name));
        let bound = entries.clone().try_fold(1024 + 8 * self.timezone.len(), |size, (name, value)| {
            let bytes = name.len().checked_add(value.map_or(0, str::len))?.checked_mul(8)?;
            size.checked_add(bytes)?.checked_add(256)
        }).ok_or_else(allocation_overflow)?;
        let reservation = self.service.memory().reserve(bound)?;
        let settings = GatewaySettings {
            parameters: entries.map(|(name, value)| (name.to_owned(), value.map(str::to_owned))).collect(),
            timezone: self.timezone.to_string(),
            access_mode: self.default_access_mode,
        };
        let encoded = serde_json::to_vec(&settings).map_err(|error| Anomaly::fault("rtdb.node/gateway-settings", error.to_string()))?;
        Ok((encoded, reservation))
    }

    pub fn restore_gateway_settings(&mut self, encoded: &[u8]) -> Result<()> {
        self.ensure_open()?;
        if self.status() != TransactionStatus::Idle {
            return Err(Anomaly::incorrect("rtdb.node/gateway-settings", "Gateway settings require an idle session"));
        }
        let memory = self.service.memory();
        let _scratch = memory.reserve(encoded.len().checked_mul(4).and_then(|bytes| bytes.checked_add(2048)).ok_or_else(allocation_overflow)?)?;
        let settings: GatewaySettings = serde_json::from_slice(encoded)
            .map_err(|error| Anomaly::incorrect("rtdb.node/gateway-settings", error.to_string()))?;
        let timezone = RetainedText::new(rtdb_expr::normalize_timezone(&settings.timezone)?, memory)?;
        let mut restored = BTreeMap::new();
        for (name, value) in settings.parameters {
            if !portable_parameter(&name) {
                return Err(Anomaly::incorrect("rtdb.node/gateway-settings", "Gateway settings contain a reserved parameter"));
            }
            if name == "bijection.at" && let Some(value) = value.as_deref() { world_clock(value)?; }
            let value = value.map(|value| RetainedText::new(value, memory)).transpose()?;
            let entry = memory.reserve(name.capacity().checked_add(256).ok_or_else(allocation_overflow)?)?;
            restored.insert(name, Parameter { value, _entry: entry });
        }
        self.parameters.retain(|name, _| !portable_parameter(name));
        self.parameters.append(&mut restored);
        self.parameters_revision = self.parameters_revision.wrapping_add(1);
        self.timezone = timezone;
        self.default_access_mode = settings.access_mode;
        self.settings_revision = self.settings_revision.wrapping_add(1);
        Ok(())
    }

    pub fn parameter(&self, name: &str) -> Option<&str> { self.parameters.get(name).and_then(|entry| entry.value.as_deref()) }
    pub fn parameter_entries(&self) -> impl Iterator<Item = (&str, Option<&str>)> + Clone { self.parameters.iter().map(|(name, entry)| (name.as_str(), entry.value.as_deref())) }
    pub fn parameters(&self) -> BTreeMap<String, Option<String>> {
        let mut parameters: BTreeMap<_, _> = self.parameter_entries().map(|(name, value)| (name.to_owned(), value.map(str::to_owned))).collect();
        parameters.insert("timezone".into(), Some(self.effective_timezone().into()));
        parameters
    }
    pub fn begin(&mut self) -> Result<()> {
        self.check_begin()?;
        let phase = match (self.parameter("bijection.at").is_some(), self.default_access_mode) {
            (true, _) | (_, Some(AccessMode::ReadOnly)) => Phase::Pending(Some(ReadOptions::default())),
            (_, Some(AccessMode::ReadWrite)) => Phase::Writing { buffer: DmlBuffer::default(), options: WriteOptions::default() },
            (_, None) => Phase::Pending(None),
        };
        self.lifecycle = Lifecycle::Open(Some(Transaction::new(phase, &self.timezone)));
        Ok(())
    }
    pub(crate) fn check_begin(&self) -> Result<()> {
        self.ensure_open()?;
        if self.transaction().is_some() { return Err(Anomaly::incorrect("xtdb/tx-already-open", "transaction already started")); }
        Ok(())
    }
    pub fn begin_with(&mut self, settings: TransactionSettings) -> Result<()> {
        self.begin()?;
        if let Err(error) = self.set_transaction(settings) { self.lifecycle = Lifecycle::Open(None); return Err(error); }
        Ok(())
    }
    pub fn set_transaction(&mut self, settings: TransactionSettings) -> Result<()> {
        self.ensure_open()?;
        if matches!(&settings.access, Some(AccessSettings::ReadWrite(_))) { self.require_open_world()?; }
        if settings.access.is_none() && settings.timezone.is_unchanged() { return Ok(()); }
        if self.transaction().is_none() && self.auto_commit { return Err(Anomaly::incorrect("xtdb/set-transaction-outside-tx", "SET TRANSACTION can only be used in a transaction")); }
        if self.transaction().is_some_and(|tx| !tx.can_shape()) { return Err(Anomaly::incorrect("xtdb/set-transaction-after-first-statement", "SET TRANSACTION must be called before any query or write")); }
        let memory = Arc::clone(self.service.memory());
        let mut created = Transaction::new(Phase::Pending(None), &self.timezone);
        let tx = self.transaction_mut().unwrap_or(&mut created);
        let timezone = rtdb_expr::normalize_timezone(&settings.timezone.apply(&Some(tx.timezone.to_string())).unwrap_or_else(|| tx.session_timezone.to_string()))?;
        let timezone = RetainedText::new(timezone, &memory)?;
        let next = match &settings.access {
            None => None,
            Some(AccessSettings::ReadOnly(settings)) => {
                let old = match &tx.phase { Phase::Pending(Some(options)) => options.clone(), _ => ReadOptions::default() };
                let options = ReadOptions { snapshot_token: update_text(&settings.snapshot_token, &old.snapshot_token, &memory)?, snapshot_time: settings.snapshot_time.apply(&old.snapshot_time),
                    clock_time: settings.clock_time.apply(&old.clock_time), await_token: update_text(&settings.await_token, &old.await_token, &memory)? };
                if let Some(token) = &options.await_token && !is_placed_token(token) { decode_tx_basis(token)?; }
                Some(Phase::Pending(Some(options)))
            }
            Some(AccessSettings::ReadWrite(settings)) => {
                let old = match &tx.phase { Phase::Writing { options, .. } => options.clone(), _ => WriteOptions::default() };
                Some(Phase::Writing { buffer: DmlBuffer::default(), options: WriteOptions {
                    system_time: settings.system_time.apply(&old.system_time), user_metadata: settings.user_metadata.apply(&old.user_metadata),
                    asynchronous: settings.asynchronous.apply(&Some(old.asynchronous)).unwrap_or(false),
                } })
            }
        };
        if let Some(phase) = next { tx.phase = phase; }
        tx.timezone = timezone;
        if self.transaction().is_none() { self.lifecycle = Lifecycle::Open(Some(created)); }
        Ok(())
    }
    pub fn set_timezone(&mut self, timezone: impl Into<String>) -> Result<()> {
        self.ensure_open()?;
        let timezone = rtdb_expr::normalize_timezone(&timezone.into())?;
        let timezone = RetainedText::new(timezone, self.service.memory())?;
        if let Some(tx) = self.transaction_mut() {
            if matches!(&tx.phase, Phase::Writing { buffer, .. } if !buffer.is_empty()) {
                return Err(Anomaly::incorrect("xtdb/set-tz-in-write-tx", "Cannot SET TIME ZONE in a read-write transaction with buffered writes"));
            }
            tx.timezone = timezone.clone(); tx.session_timezone = timezone;
        } else { self.replace_timezone(timezone); }
        Ok(())
    }
    fn replace_timezone(&mut self, timezone: RetainedText) {
        if &*self.timezone != &*timezone {
            self.timezone = timezone;
            self.settings_revision = self.settings_revision.wrapping_add(1);
        }
    }
    pub fn fail_transaction(&mut self) { if let Some(tx) = self.transaction_mut() { tx.failed = true; } }
    pub fn rollback_tx(&mut self) -> Result<()> { self.ensure_open()?; self.lifecycle = Lifecycle::Open(None); Ok(()) }
    pub fn rollback(&mut self) -> Result<()> {
        if self.auto_commit { return Err(Anomaly::incorrect("xtdb.adbc/rollback-in-autocommit", "Cannot rollback when autoCommit is enabled")); }
        self.rollback_tx()
    }
    pub fn reset(&mut self) -> Result<()> {
        self.ensure_open()?;
        if self.transaction().is_some() { return Err(Anomaly::incorrect("xtdb/discard-in-tx", "DISCARD ALL cannot run inside a transaction block")); }
        self.parameters.retain(|name, _| name == "application_name");
        self.parameters_revision = self.parameters_revision.wrapping_add(1);
        self.timezone = self.initial_timezone.clone();
        self.replace_await_token(None);
        self.last_submitted = None;
        self.default_access_mode = None;
        self.settings_revision = self.settings_revision.wrapping_add(1);
        Ok(())
    }
    pub async fn set_auto_commit(&mut self, enabled: bool) -> Result<()> {
        self.ensure_open()?;
        if enabled == self.auto_commit { return Ok(()); }
        if enabled { self.commit_tx(None).await?; }
        self.auto_commit = enabled;
        Ok(())
    }
    async fn pin(&self, options: &ReadOptions) -> Result<(QueryBasis, Option<String>)> {
        let token = options.await_token.as_deref().or(self.await_token.as_deref());
        let world_time = self.parameter("bijection.at").map(world_clock).transpose()?;
        let has_await_floor = token.is_some();
        if options.snapshot_token.is_some() && (has_await_floor || options.snapshot_time.is_some() || options.clock_time.is_some() || world_time.is_some()) {
            return Err(crate::sql_snapshots::unretained());
        }
        let applied = self.service.await_client_token(token, Duration::from_secs(60), &self.cancellation).await?;
        let current_time = match world_time.or(options.clock_time) {
            Some(time) => time,
            None if options.snapshot_token.is_some() => Instant::default(),
            None => self.cancellation.run_until_cancelled(self.service.fresh_query_instant()).await
                .ok_or_else(|| Anomaly::new(rtdb_types::AnomalyCategory::Interrupted, "rtdb/cancelled", "Snapshot admission was cancelled"))??,
        };
        let (snapshot, replica_capture, resume_token) = match &options.snapshot_token {
            Some(snapshot_token) => (TimeBasis::default(), None, Some(snapshot_token.to_string())),
            None => {
                let (basis, capture) = self.service.fresh_snapshot_basis(&self.cancellation).await?;
                (basis, capture, None)
            }
        };
        Ok((QueryBasis { snapshot, replica_capture, resume_token, has_await_floor, snapshot_time: options.snapshot_time, current_time, timezone: self.effective_timezone().to_owned() }, applied))
    }
    pub async fn read_basis(&mut self) -> Result<QueryBasis> {
        self.revalidate().await?;
        self.require_usable_transaction()?;
        if let Some(tx) = self.transaction() {
            match &tx.phase {
                Phase::Writing { .. } => return Err(Anomaly::incorrect("xtdb/queries-in-read-write-tx", "Queries are unsupported in a DML transaction")),
                Phase::Reading { basis, .. } => { let mut basis = basis.basis.clone(); basis.timezone = tx.timezone.to_string(); return Ok(basis); }
                Phase::Pending(_) => (),
            }
        }
        let options = match self.transaction().map(|tx| &tx.phase) { Some(Phase::Pending(Some(options))) => options.clone(), _ => ReadOptions::default() };
        let (basis, applied) = self.pin(&options).await?;
        let mut local_token = options.await_token;
        if let Some(token) = applied {
            if local_token.is_some() { local_token = Some(RetainedText::new(token, self.service.memory())?); }
            else { self.set_await_token(Some(token))?; }
        }
        if self.transaction().is_some() || !self.auto_commit {
            let retained = RetainedBasis::copy(&basis, self.service.memory())?;
            if let Some(tx) = self.transaction_mut() { tx.phase = Phase::Reading { basis: retained, await_token: local_token }; }
            else { self.lifecycle = Lifecycle::Open(Some(Transaction::new(Phase::Reading { basis: retained, await_token: None }, &self.timezone))); }
        }
        Ok(basis)
    }
    pub async fn prepare_basis(&self) -> Result<QueryBasis> {
        self.revalidate().await?;
        self.require_usable_transaction()?;
        let options = match self.transaction().map(|tx| &tx.phase) {
            Some(Phase::Pending(Some(options))) => options.clone(),
            Some(Phase::Reading { basis, await_token }) => {
                self.service.await_client_token(await_token.as_deref().or(self.await_token.as_deref()), Duration::from_secs(60), &self.cancellation).await?;
                let mut basis = basis.basis.clone();
                basis.timezone = self.effective_timezone().to_owned();
                return Ok(basis);
            }
            _ => ReadOptions::default(),
        };
        self.pin(&options).await.map(|(basis, _)| basis)
    }
    pub async fn unchecked_read_basis(&mut self) -> Result<QueryBasis> {
        if matches!(self.transaction().map(|tx| &tx.phase), Some(Phase::Pending(Some(_)) | Phase::Reading { .. })) {
            self.read_basis().await
        } else {
            self.prepare_basis().await
        }
    }
    /// Captures SHOW's basis without awaiting writes or resolving a pending transaction.
    /// Callers revalidate the session before executing the inspection; failed transactions remain inspectable.
    pub fn inspection_basis(&self) -> Result<QueryBasis> {
        self.ensure_open()?;
        if let Some(Transaction { phase: Phase::Reading { basis, .. }, .. }) = self.transaction() {
            let mut basis = basis.basis.clone();
            basis.timezone = self.effective_timezone().to_owned();
            return Ok(basis);
        }
        let current_time = self.parameter("bijection.at").map(world_clock).transpose()?
            .unwrap_or_else(|| self.service.query_instant());
        Ok(QueryBasis { snapshot: self.service.snapshot_basis()?, replica_capture: None, resume_token: None, has_await_floor: false, snapshot_time: None, current_time, timezone: self.effective_timezone().to_owned() })
    }
    pub(crate) fn require_open_world(&self) -> Result<()> {
        if self.parameter("bijection.at").is_some() {
            return Err(Anomaly::incorrect("xtdb/read-only-tx", "Cannot write with a fixed bijection.at"));
        }
        Ok(())
    }
    pub async fn execute_operation(&mut self, operation: TxOp) -> Result<Option<TransactionResult>> {
        self.revalidate().await?;
        self.require_usable_transaction()?;
        self.require_open_world()?;
        if self.auto_commit && self.transaction().is_none() {
            let options = SubmitOptions { default_tz: self.timezone.to_string(), ..SubmitOptions::default() };
            let result = self.submit_operations(vec![operation], options, CommitMode::Sync).await?;
            let CommitResult::Executed(result) = result else { return Err(Anomaly::fault("rtdb.node/commit-mode", "Synchronous submission returned no transaction result")); };
            if let Some(error) = result.error() { return Err(error.clone()); }
            return Ok(Some(result));
        }
        if self.transaction().is_none() { self.lifecycle = Lifecycle::Open(Some(Transaction::new(Phase::Pending(None), &self.timezone))); }
        let memory = Arc::clone(self.service.memory());
        let tx = self.transaction_mut().ok_or_else(|| Anomaly::fault("rtdb.node/transaction-state", "Buffered operation has no open transaction"))?;
        if matches!(&tx.phase, Phase::Pending(None)) { tx.phase = Phase::Writing { buffer: DmlBuffer::default(), options: WriteOptions::default() }; }
        match &mut tx.phase {
            Phase::Writing { buffer, .. } => buffer.append(operation, &memory)?,
            _ => return Err(Anomaly::incorrect("xtdb/read-only-tx", "Cannot write in a read-only transaction")),
        }
        Ok(None)
    }
    async fn submit_operations(&mut self, operations: Vec<TxOp>, options: SubmitOptions, mode: CommitMode) -> Result<CommitResult> {
        self.submit_to(self.database.to_string(), operations, options, mode, Submission::Ordinary).await
    }
    async fn submit_to(&mut self, database: String, operations: Vec<TxOp>, mut options: SubmitOptions, mode: CommitMode, submission: Submission) -> Result<CommitResult> {
        let observer = self.service.execution_observer();
        let _sample = rtdb_plan::ExecutionSample::new(observer.clone(), match mode { CommitMode::Async => rtdb_plan::ExecutionTimer::TxSubmit, CommitMode::Sync => rtdb_plan::ExecutionTimer::TxExecute });
        self.revalidate().await?;
        self.ensure_open()?;
        self.require_open_world()?;
        if self.default_access_mode == Some(AccessMode::ReadOnly)
            && operations.iter().any(TxOp::is_owned)
        {
            return Err(Anomaly::incorrect("xtdb/read-only-tx", "Cannot write an owned relation in a read-only session"));
        }
        self.service.check_submission_basis(&database, self.await_token.as_deref(), &self.cancellation).await?;
        options.user = self.service.user().map(str::to_owned);
        if options.default_tz.is_empty() { options.default_tz = self.timezone.to_string(); }
        let ordinary = matches!(&submission, Submission::Ordinary);
        let reserve_present = matches!(&submission, Submission::PresentReservation);
        if ordinary && let Some((key, sequence)) = &mut self.gateway_key {
            options.user_metadata = Some(crate::node::with_idempotency_key(options.user_metadata.take(), &format!("{}/{sequence}", &**key), self.service.memory())?);
            *sequence += 1;
        } else if ordinary && let Some(key) = self.parameter(crate::node::IDEMPOTENCY_KEY_PARAMETER).filter(|key| !key.is_empty()).map(str::to_owned) {
            let sequence = match &mut self.idempotency_sequence { Some((current, next)) if *current == key => { *next += 1; *next - 1 } sequence => { *sequence = Some((key.clone(), 1)); 0 } };
            options.user_metadata = Some(crate::node::with_idempotency_key(options.user_metadata.take(), &format!("{key}/{sequence}"), self.service.memory())?);
        }
        let submitted = match submission {
            Submission::Replay(permit) => self.service.submit_replay(&database, operations, options, permit, &self.cancellation).await,
            Submission::Ordinary | Submission::PresentReservation => self.service.submit(&database, operations, options, &self.cancellation).await,
        }.inspect_err(|_| {
            if let Some(observer) = &observer { observer.record_error(rtdb_plan::ExecutionError::Transaction); }
        })?;
        let token = if let Some(receipt) = &submitted.placed_receipt {
            receipt.clone()
        } else {
            let next = TxBasis { databases: BTreeMap::from([(database.clone(), vec![submitted.tx_id])]) };
            let token = match &self.await_token { Some(token) => decode_tx_basis(token)?.merge(&next)?, None => next };
            encode_tx_basis(&token)
        };
        match mode {
            CommitMode::Async => {
                self.set_await_token(Some(token))?;
                self.last_submitted = Some(LastSubmittedTx::Submitted(submitted.clone()));
                Ok(CommitResult::Submitted(submitted))
            }
            CommitMode::Sync => {
                let (result, applied) = {
                    let _await = rtdb_plan::ExecutionSample::new(observer.clone(), rtdb_plan::ExecutionTimer::TxAwait);
                    self.service.await_submission(&database, &submitted, &self.cancellation).await?
                };
                if reserve_present && result.tx_key().tx_id != submitted.tx_id {
                    return Err(Anomaly::fault("rtdb.node/present-reservation", "The ordered present resolved at a different source position"));
                }
                if matches!(result, TransactionResult::Aborted { .. }) { if let Some(observer) = &observer { observer.record_error(rtdb_plan::ExecutionError::Transaction); } }
                self.set_await_token(Some(applied.unwrap_or(token)))?;
                self.last_submitted = Some(LastSubmittedTx::Executed(result.clone()));
                Ok(CommitResult::Executed(result))
            }
        }
    }
    async fn autonomous(&mut self, operations: Vec<TxOp>, options: AutonomousTxOptions, mode: CommitMode) -> Result<CommitResult> {
        self.ensure_open()?;
        if self.transaction().is_some() { return Err(Anomaly::incorrect("xtdb/tx-already-in-progress", "Cannot submit a transaction while another is in progress on this connection - commit or roll back first")); }
        let timezone = rtdb_expr::normalize_timezone(options.default_timezone.as_deref().unwrap_or(&self.timezone))?;
        let database = options.database.unwrap_or_else(|| self.database.to_string());
        rtdb_types::boxed_with(self, |this| this.submit_to(database, operations,
            SubmitOptions { default_tz: timezone, system_time: options.system_time, user: None, user_metadata: options.user_metadata }, mode, Submission::Ordinary)).await
    }
    pub async fn submit_tx(&mut self, operations: Vec<TxOp>, options: AutonomousTxOptions) -> Result<SubmittedTx> {
        match self.autonomous(operations, options, CommitMode::Async).await? {
            CommitResult::Submitted(result) => Ok(result),
            _ => Err(Anomaly::fault("rtdb.node/submit-result", "Asynchronous submission returned no source position")),
        }
    }
    pub async fn execute_tx(&mut self, operations: Vec<TxOp>, options: AutonomousTxOptions) -> Result<TransactionResult> {
        match rtdb_types::boxed_with(&mut *self, |this| this.autonomous(operations, options, CommitMode::Sync)).await? {
            CommitResult::Executed(result) => Ok(result),
            _ => Err(Anomaly::fault("rtdb.node/execute-result", "Synchronous execution returned no transaction result")),
        }
    }
    pub(crate) async fn reserve_ordered_present(&mut self) -> Result<TransactionKey> {
        self.ensure_open()?;
        if self.default_access_mode == Some(AccessMode::ReadOnly) || self.is_read_only() {
            return Err(Anomaly::incorrect("xtdb/read-only-tx", "Cannot reserve an ordered present in a read-only session or transaction"));
        }
        if !self.auto_commit || self.transaction().is_some() {
            return Err(Anomaly::incorrect("xtdb/tx-already-in-progress", "Ordered present reservation requires an idle session in autocommit mode"));
        }
        let metadata = UserMetadata::new(
            Value::Struct(rtdb_types::StructFields::from([("rtdb_present_reservation".into(), Value::Bool(true))])),
            self.service.memory(),
        )?;
        let options = SubmitOptions { user_metadata: Some(metadata), default_tz: self.timezone.to_string(), ..SubmitOptions::default() };
        let result = self.submit_to(
            self.database.to_string(), Vec::new(), options, CommitMode::Sync, Submission::PresentReservation,
        ).await?;
        match result {
            CommitResult::Executed(TransactionResult::Committed { tx_key }) => Ok(tx_key),
            CommitResult::Executed(TransactionResult::Aborted { error: Some(error), .. }) => Err((*error).clone()),
            _ => Err(Anomaly::fault("rtdb.node/present-reservation", "The ordered present transaction did not commit")),
        }
    }

    pub async fn commit_tx(&mut self, mode: Option<CommitMode>) -> Result<CommitResult> {
        self.commit_tx_inner(mode, None).await
    }

    /// Commits one buffered write synchronously under a trusted frontend's private permit.
    /// A duplicate returns the original committed key when resolution reaches its marker.
    /// An empty or multi-operation write buffer is rejected.
    pub async fn commit_tx_with_replay_permit(&mut self, permit: ReplaySubmissionPermit) -> Result<CommitResult> {
        self.commit_tx_inner(Some(CommitMode::Sync), Some(permit)).await
    }

    async fn commit_tx_inner(&mut self, mode: Option<CommitMode>, replay: Option<ReplaySubmissionPermit>) -> Result<CommitResult> {
        self.revalidate().await?;
        self.require_usable_transaction()?;
        let Lifecycle::Open(tx) = &mut self.lifecycle else { return Err(Anomaly::incorrect("xtdb/connection-closed", "Connection is closed")); };
        let Some(tx) = tx.take() else {
            return if replay.is_some() { Err(replay_operation_count()) } else { Ok(CommitResult::NoTransaction) };
        };
        self.replace_timezone(tx.session_timezone);
        let Phase::Writing { buffer, options } = tx.phase else {
            return if replay.is_some() { Err(replay_operation_count()) } else { Ok(CommitResult::NoTransaction) };
        };
        if replay.is_some() && buffer.operations.len() != 1 { return Err(replay_operation_count()); }
        let mode = mode.unwrap_or(if options.asynchronous { CommitMode::Async } else { CommitMode::Sync });
        let (operations, reservations): (Vec<_>, Vec<_>) = buffer.operations.into_iter().map(|entry| (entry.operation, entry._reservation)).unzip();
        let result = self.submit_to(self.database.to_string(), operations, SubmitOptions {
            default_tz: tx.timezone.to_string(), system_time: options.system_time,
            user: None, user_metadata: options.user_metadata,
        }, mode, replay.map_or(Submission::Ordinary, Submission::Replay)).await;
        drop(reservations);
        result
    }
    pub async fn commit(&mut self) -> Result<CommitResult> {
        if self.auto_commit { return Err(Anomaly::incorrect("xtdb.adbc/commit-in-autocommit", "Cannot commit when autoCommit is enabled")); }
        let result = self.commit_tx(None).await?;
        if let CommitResult::Executed(tx) = &result { if let Some(error) = tx.error() { return Err(error.clone()); } }
        Ok(result)
    }
    pub fn close(&mut self) { self.cancellation.cancel(); self.lifecycle = Lifecycle::Closed; self.parameters.clear(); self.parameters_revision = self.parameters_revision.wrapping_add(1); self.await_token = None; self.last_submitted = None; }
}

impl Drop for Session { fn drop(&mut self) { self.cancellation.cancel(); } }

fn update_text(update: &OptionUpdate<String>, old: &Option<RetainedText>, memory: &Arc<MemoryBudget>) -> Result<Option<RetainedText>> {
    match update {
        OptionUpdate::Unchanged => Ok(old.clone()), OptionUpdate::Clear => Ok(None),
        OptionUpdate::Set(value) => { let reservation = memory.reserve(value.len().checked_add(128).ok_or_else(allocation_overflow)?)?; Ok(Some(RetainedText { value: Arc::new(value.clone()), _reservation: reservation })) }
    }
}
fn allocation_overflow() -> Anomaly { Anomaly::incorrect("rtdb.node/session-allocation", "Session allocation size exceeds usize") }

fn replay_operation_count() -> Anomaly {
    Anomaly::incorrect("rtdb.node/private-replay-operation-count", "Private replay requires exactly one buffered write operation")
}
