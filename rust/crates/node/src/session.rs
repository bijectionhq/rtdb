use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use rtdb_columnar::{MemoryBudget, MemoryReservation};
use rtdb_format::basis::{decode_time_basis, decode_tx_basis, encode_time_basis, encode_tx_basis};
use rtdb_plan::AccessMode;
use rtdb_tx::{SubmitOptions, SubmittedTx, TransactionResult, TxOp, UserMetadata};
use rtdb_types::{Anomaly, Result, TimeBasis, Instant, Timestamp, TxBasis, TxId};
use tokio_util::sync::CancellationToken;

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
    fn snapshot_basis(&self) -> Result<TimeBasis>;
    async fn await_basis(&self, basis: Option<TxBasis>, timeout: Duration, cancellation: &CancellationToken) -> Result<()>;
    async fn submit(&self, database: &str, operations: Vec<TxOp>, options: SubmitOptions, cancellation: &CancellationToken) -> Result<SubmittedTx>;
    async fn await_transaction(&self, database: &str, tx_id: TxId, cancellation: &CancellationToken) -> Result<TransactionResult>;
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct QueryBasis {
    pub snapshot: TimeBasis,
    pub snapshot_time: Option<Timestamp>,
    pub current_time: Instant,
    pub timezone: String,
}
impl QueryBasis {
    pub fn snapshot_token(&self) -> String { encode_time_basis(&self.snapshot) }
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

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum CommitMode { Sync, Async }

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
            TxOp::Sql { sql, .. } | TxOp::SqlBytes { sql, .. } => sql.capacity(),
            TxOp::PutDocs { table, .. } | TxOp::PatchDocs { table, .. } | TxOp::DeleteDocs { table, .. } | TxOp::EraseDocs { table, .. } => table.schema.capacity().checked_add(table.table.capacity()).ok_or_else(allocation_overflow)?,
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
        let mut bytes = basis.timezone.capacity().checked_add(256).ok_or_else(allocation_overflow)?;
        for (name, values) in &basis.snapshot.databases {
            bytes = bytes.checked_add(name.capacity()).and_then(|bytes| bytes.checked_add(values.capacity().checked_mul(std::mem::size_of::<Option<rtdb_types::Instant>>())?)).and_then(|bytes| bytes.checked_add(256)).ok_or_else(allocation_overflow)?;
        }
        let reservation = memory.reserve(bytes)?;
        Ok(Self { basis: basis.clone(), _reservation: reservation })
    }
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
    last_submitted: Option<LastSubmittedTx>,
    parameters: BTreeMap<String, Parameter>,
    cancellation: CancellationToken,
}

impl Session {
    pub fn new(service: Arc<dyn SessionService>, database: impl Into<String>, timezone: impl Into<String>, parent: &CancellationToken) -> Result<Self> {
        let timezone = rtdb_expr::normalize_timezone(&timezone.into())?;
        service.check_open()?;
        let database = RetainedText::new(database.into(), service.memory())?;
        let timezone = RetainedText::new(timezone, service.memory())?;
        Ok(Self { service, database, lifecycle: Lifecycle::Open(None), auto_commit: true, default_access_mode: None,
            initial_timezone: timezone.clone(), timezone, await_token: None, last_submitted: None, parameters: BTreeMap::new(), cancellation: parent.child_token() })
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
        if self.transaction().is_some_and(|tx| tx.failed) { return Err(Anomaly::incorrect("xtdb/tx-failed", "Current transaction is aborted; roll back before issuing more statements")); }
        Ok(())
    }
    pub fn database(&self) -> &str { &self.database }
    pub fn set_database(&mut self, database: impl Into<String>) -> Result<()> { self.ensure_open()?; self.database = RetainedText::new(database.into(), self.service.memory())?; Ok(()) }
    pub fn user(&self) -> Option<&str> { self.service.user() }
    pub fn auto_commit(&self) -> bool { self.auto_commit }
    pub fn effective_timezone(&self) -> &str { self.transaction().map_or(&self.timezone, |tx| &tx.timezone) }
    pub fn session_timezone(&self) -> &str { self.transaction().map_or(&self.timezone, |tx| &tx.session_timezone) }
    pub fn effective_await_token(&self) -> Option<&str> { self.transaction().and_then(Transaction::await_token).or(self.await_token.as_deref()) }
    pub fn last_submitted(&self) -> Option<&LastSubmittedTx> { self.last_submitted.as_ref() }
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
    pub fn set_await_token(&mut self, token: Option<String>) -> Result<()> { self.ensure_open()?; self.await_token = token.map(|token| RetainedText::new(token, self.service.memory())).transpose()?; Ok(()) }
    pub fn set_session_characteristics(&mut self, access: Option<AccessMode>) -> Result<()> { self.ensure_open()?; self.default_access_mode = access; Ok(()) }
    pub fn set_parameter(&mut self, name: impl Into<String>, value: Option<String>) -> Result<()> {
        self.ensure_open()?;
        let name = name.into();
        let value = value.map(|value| RetainedText::new(value, self.service.memory())).transpose()?;
        let entry = self.service.memory().reserve(name.capacity().checked_add(256).ok_or_else(allocation_overflow)?)?;
        self.parameters.insert(name, Parameter { value, _entry: entry });
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
        let phase = match self.default_access_mode {
            Some(AccessMode::ReadOnly) => Phase::Pending(Some(ReadOptions::default())),
            Some(AccessMode::ReadWrite) => Phase::Writing { buffer: DmlBuffer::default(), options: WriteOptions::default() },
            None => Phase::Pending(None),
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
                if let Some(token) = &options.await_token { decode_tx_basis(token)?; }
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
        } else { self.timezone = timezone; }
        Ok(())
    }
    pub fn fail_transaction(&mut self, _: Anomaly) { if let Some(tx) = self.transaction_mut() { tx.failed = true; } }
    pub fn rollback_tx(&mut self) -> Result<()> { self.ensure_open()?; self.lifecycle = Lifecycle::Open(None); Ok(()) }
    pub fn rollback(&mut self) -> Result<()> {
        if self.auto_commit { return Err(Anomaly::incorrect("xtdb.adbc/rollback-in-autocommit", "Cannot rollback when autoCommit is enabled")); }
        self.rollback_tx()
    }
    pub fn reset(&mut self) -> Result<()> {
        self.ensure_open()?;
        if self.transaction().is_some() { return Err(Anomaly::incorrect("xtdb/discard-in-tx", "DISCARD ALL cannot run inside a transaction block")); }
        self.parameters.clear(); self.timezone = self.initial_timezone.clone(); self.await_token = None; self.default_access_mode = None;
        Ok(())
    }
    pub async fn set_auto_commit(&mut self, enabled: bool) -> Result<()> {
        self.ensure_open()?;
        if enabled == self.auto_commit { return Ok(()); }
        if enabled { self.commit_tx(None).await?; }
        self.auto_commit = enabled;
        Ok(())
    }
    async fn pin(&self, options: &ReadOptions) -> Result<QueryBasis> {
        let token = options.await_token.as_deref().or(self.await_token.as_deref());
        self.service.await_basis(token.map(decode_tx_basis).transpose()?, Duration::from_secs(60), &self.cancellation).await?;
        let snapshot = match &options.snapshot_token { Some(token) => decode_time_basis(token)?, None => self.service.snapshot_basis()? };
        Ok(QueryBasis { snapshot, snapshot_time: options.snapshot_time, current_time: options.clock_time.unwrap_or_else(|| self.service.query_instant()), timezone: self.effective_timezone().to_owned() })
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
        let basis = self.pin(&options).await?;
        if self.transaction().is_some() || !self.auto_commit {
            let retained = RetainedBasis::copy(&basis, self.service.memory())?;
            if let Some(tx) = self.transaction_mut() { tx.phase = Phase::Reading { basis: retained, await_token: options.await_token }; }
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
                self.service.await_basis(await_token.as_deref().or(self.await_token.as_deref()).map(decode_tx_basis).transpose()?, Duration::from_secs(60), &self.cancellation).await?;
                let mut basis = basis.basis.clone();
                basis.timezone = self.effective_timezone().to_owned();
                return Ok(basis);
            }
            _ => ReadOptions::default(),
        };
        self.pin(&options).await
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
        Ok(QueryBasis { snapshot: self.service.snapshot_basis()?, snapshot_time: None, current_time: self.service.query_instant(), timezone: self.effective_timezone().to_owned() })
    }
    pub async fn execute_operation(&mut self, operation: TxOp) -> Result<Option<TransactionResult>> {
        self.revalidate().await?;
        self.require_usable_transaction()?;
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
        self.submit_to(self.database.to_string(), operations, options, mode).await
    }
    async fn submit_to(&mut self, database: String, operations: Vec<TxOp>, mut options: SubmitOptions, mode: CommitMode) -> Result<CommitResult> {
        let observer = self.service.execution_observer();
        let _sample = rtdb_plan::ExecutionSample::new(observer.clone(), match mode { CommitMode::Async => rtdb_plan::ExecutionTimer::TxSubmit, CommitMode::Sync => rtdb_plan::ExecutionTimer::TxExecute });
        self.revalidate().await?;
        self.ensure_open()?;
        options.user = self.service.user().map(str::to_owned);
        if options.default_tz.is_empty() { options.default_tz = self.timezone.to_string(); }
        let submitted = self.service.submit(&database, operations, options, &self.cancellation).await.inspect_err(|_| {
            if let Some(observer) = &observer { observer.record_error(rtdb_plan::ExecutionError::Transaction); }
        })?;
        let next = TxBasis { databases: BTreeMap::from([(database.clone(), vec![submitted.tx_id])]) };
        let token = match &self.await_token { Some(token) => decode_tx_basis(token)?.merge(&next)?, None => next };
        let token = RetainedText::new(encode_tx_basis(&token), self.service.memory())?;
        match mode {
            CommitMode::Async => { self.await_token = Some(token); self.last_submitted = Some(LastSubmittedTx::Submitted(submitted)); Ok(CommitResult::Submitted(submitted)) }
            CommitMode::Sync => {
                let result = {
                    let _await = rtdb_plan::ExecutionSample::new(observer.clone(), rtdb_plan::ExecutionTimer::TxAwait);
                    self.service.await_transaction(&database, submitted.tx_id, &self.cancellation).await?
                };
                if matches!(result, TransactionResult::Aborted { .. }) { if let Some(observer) = &observer { observer.record_error(rtdb_plan::ExecutionError::Transaction); } }
                self.await_token = Some(token); self.last_submitted = Some(LastSubmittedTx::Executed(result.clone()));
                Ok(CommitResult::Executed(result))
            }
        }
    }
    async fn autonomous(&mut self, operations: Vec<TxOp>, options: AutonomousTxOptions, mode: CommitMode) -> Result<CommitResult> {
        self.ensure_open()?;
        if self.transaction().is_some() { return Err(Anomaly::incorrect("xtdb/tx-already-in-progress", "Cannot submit a transaction while another is in progress on this connection - commit or roll back first")); }
        let timezone = rtdb_expr::normalize_timezone(options.default_timezone.as_deref().unwrap_or(&self.timezone))?;
        self.submit_to(options.database.unwrap_or_else(|| self.database.to_string()), operations,
            SubmitOptions { default_tz: timezone, system_time: options.system_time, user: None, user_metadata: options.user_metadata }, mode).await
    }
    pub async fn submit_tx(&mut self, operations: Vec<TxOp>, options: AutonomousTxOptions) -> Result<SubmittedTx> {
        match self.autonomous(operations, options, CommitMode::Async).await? {
            CommitResult::Submitted(result) => Ok(result),
            _ => Err(Anomaly::fault("rtdb.node/submit-result", "Asynchronous submission returned no source position")),
        }
    }
    pub async fn execute_tx(&mut self, operations: Vec<TxOp>, options: AutonomousTxOptions) -> Result<TransactionResult> {
        match self.autonomous(operations, options, CommitMode::Sync).await? {
            CommitResult::Executed(result) => Ok(result),
            _ => Err(Anomaly::fault("rtdb.node/execute-result", "Synchronous execution returned no transaction result")),
        }
    }
    pub async fn commit_tx(&mut self, mode: Option<CommitMode>) -> Result<CommitResult> {
        self.revalidate().await?;
        self.require_usable_transaction()?;
        let Lifecycle::Open(tx) = &mut self.lifecycle else { return Err(Anomaly::incorrect("xtdb/connection-closed", "Connection is closed")); };
        let Some(tx) = tx.take() else { return Ok(CommitResult::NoTransaction); };
        self.timezone = tx.session_timezone;
        let Phase::Writing { buffer, options } = tx.phase else { return Ok(CommitResult::NoTransaction); };
        let mode = mode.unwrap_or(if options.asynchronous { CommitMode::Async } else { CommitMode::Sync });
        let (operations, reservations): (Vec<_>, Vec<_>) = buffer.operations.into_iter().map(|entry| (entry.operation, entry._reservation)).unzip();
        let result = self.submit_operations(operations, SubmitOptions { default_tz: tx.timezone.to_string(), system_time: options.system_time,
            user: None, user_metadata: options.user_metadata }, mode).await;
        drop(reservations);
        result
    }
    pub async fn commit(&mut self) -> Result<CommitResult> {
        if self.auto_commit { return Err(Anomaly::incorrect("xtdb.adbc/commit-in-autocommit", "Cannot commit when autoCommit is enabled")); }
        let result = self.commit_tx(None).await?;
        if let CommitResult::Executed(tx) = &result { if let Some(error) = tx.error() { return Err(error.clone()); } }
        Ok(result)
    }
    pub fn close(&mut self) { self.cancellation.cancel(); self.lifecycle = Lifecycle::Closed; self.parameters.clear(); self.await_token = None; self.last_submitted = None; }
}

impl Drop for Session { fn drop(&mut self) { self.cancellation.cancel(); } }

fn update_text(update: &OptionUpdate<String>, old: &Option<RetainedText>, memory: &Arc<MemoryBudget>) -> Result<Option<RetainedText>> {
    match update {
        OptionUpdate::Unchanged => Ok(old.clone()), OptionUpdate::Clear => Ok(None),
        OptionUpdate::Set(value) => { let reservation = memory.reserve(value.len().checked_add(128).ok_or_else(allocation_overflow)?)?; Ok(Some(RetainedText { value: Arc::new(value.clone()), _reservation: reservation })) }
    }
}
fn allocation_overflow() -> Anomaly { Anomaly::incorrect("rtdb.node/session-allocation", "Session allocation size exceeds usize") }
