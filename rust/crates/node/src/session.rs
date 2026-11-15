use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use rtdb_format::basis::{decode_time_basis, decode_tx_basis, encode_time_basis, encode_tx_basis};
use rtdb_plan::AccessMode;
use rtdb_tx::{SubmitOptions, SubmittedTx, TransactionResult, TxOp, UserMetadata};
use rtdb_types::{Anomaly, Result, TimeBasis, Timestamp, TxBasis, TxId};
use tokio_util::sync::CancellationToken;

#[async_trait]
pub trait SessionService: Send + Sync {
    fn user(&self) -> Option<&str>;
    fn check_open(&self) -> Result<()>;
    async fn revalidate(&self) -> Result<()>;
    fn now(&self) -> Timestamp;
    fn snapshot_basis(&self) -> Result<TimeBasis>;
    async fn await_basis(&self, basis: Option<TxBasis>, timeout: Duration, cancellation: &CancellationToken) -> Result<()>;
    async fn submit(&self, database: &str, operations: Vec<TxOp>, options: SubmitOptions, cancellation: &CancellationToken) -> Result<SubmittedTx>;
    async fn await_transaction(&self, database: &str, tx_id: TxId, cancellation: &CancellationToken) -> Result<TransactionResult>;
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct QueryBasis {
    pub snapshot: TimeBasis,
    pub snapshot_time: Option<Timestamp>,
    pub current_time: Timestamp,
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
    pub clock_time: OptionUpdate<Timestamp>,
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
    snapshot_token: Option<String>,
    snapshot_time: Option<Timestamp>,
    clock_time: Option<Timestamp>,
    await_token: Option<String>,
}

#[derive(Clone, Debug, Default)]
struct WriteOptions {
    system_time: Option<Timestamp>,
    user_metadata: Option<UserMetadata>,
    asynchronous: bool,
}

#[derive(Default)]
struct DmlBuffer { operations: Vec<TxOp> }
impl DmlBuffer {
    fn append(&mut self, operation: TxOp) -> Result<()> {
        match (self.operations.last_mut(), operation) {
            (Some(TxOp::Sql { sql: previous, args: Some(target) }), TxOp::Sql { sql, args: Some(args) }) if *previous == sql => target.append(args),
            (_, operation) => { self.operations.push(operation); Ok(()) }
        }
    }
    fn is_empty(&self) -> bool { self.operations.is_empty() }
}

enum Phase {
    Pending(Option<ReadOptions>),
    Reading { basis: QueryBasis, await_token: Option<String> },
    Writing { buffer: DmlBuffer, options: WriteOptions },
}

struct Transaction {
    phase: Phase,
    failure: Option<Anomaly>,
    timezone: String,
    session_timezone: String,
}
impl Transaction {
    fn new(phase: Phase, timezone: &str) -> Self { Self { phase, failure: None, timezone: timezone.to_owned(), session_timezone: timezone.to_owned() } }
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

pub struct Session {
    service: Arc<dyn SessionService>,
    database: String,
    lifecycle: Lifecycle,
    auto_commit: bool,
    default_access_mode: Option<AccessMode>,
    timezone: String,
    initial_timezone: String,
    await_token: Option<String>,
    last_submitted: Option<LastSubmittedTx>,
    parameters: BTreeMap<String, Option<String>>,
    cancellation: CancellationToken,
}

impl Session {
    pub fn new(service: Arc<dyn SessionService>, database: impl Into<String>, timezone: impl Into<String>, parent: &CancellationToken) -> Result<Self> {
        let timezone = rtdb_expr::normalize_timezone(&timezone.into())?;
        service.check_open()?;
        Ok(Self { service, database: database.into(), lifecycle: Lifecycle::Open(None), auto_commit: true, default_access_mode: None,
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
        if self.transaction().is_some_and(|tx| tx.failure.is_some()) { return Err(Anomaly::incorrect("xtdb/tx-failed", "Current transaction is aborted; roll back before issuing more statements")); }
        Ok(())
    }
    pub fn database(&self) -> &str { &self.database }
    pub fn set_database(&mut self, database: impl Into<String>) -> Result<()> { self.ensure_open()?; self.database = database.into(); Ok(()) }
    pub fn user(&self) -> Option<&str> { self.service.user() }
    pub fn auto_commit(&self) -> bool { self.auto_commit }
    pub fn effective_timezone(&self) -> &str { self.transaction().map_or(&self.timezone, |tx| &tx.timezone) }
    pub fn effective_await_token(&self) -> Option<&str> { self.transaction().and_then(Transaction::await_token).or(self.await_token.as_deref()) }
    pub fn last_submitted(&self) -> Option<&LastSubmittedTx> { self.last_submitted.as_ref() }
    pub fn cancellation(&self) -> &CancellationToken { &self.cancellation }
    pub fn status(&self) -> TransactionStatus {
        match &self.lifecycle {
            Lifecycle::Closed => TransactionStatus::Closed,
            Lifecycle::Open(None) => TransactionStatus::Idle,
            Lifecycle::Open(Some(tx)) if tx.failure.is_some() => TransactionStatus::Failed,
            Lifecycle::Open(Some(tx)) => match &tx.phase { Phase::Pending(_) => TransactionStatus::Pending, Phase::Reading { .. } => TransactionStatus::Reading, Phase::Writing { .. } => TransactionStatus::Writing },
        }
    }
    pub fn is_read_only(&self) -> bool { matches!(self.transaction().map(|tx| &tx.phase), Some(Phase::Reading { .. } | Phase::Pending(Some(_)))) }
    pub fn is_read_write(&self) -> bool { matches!(self.transaction().map(|tx| &tx.phase), Some(Phase::Writing { .. })) }
    pub fn set_await_token(&mut self, token: Option<String>) -> Result<()> { self.ensure_open()?; self.await_token = token; Ok(()) }
    pub fn set_session_characteristics(&mut self, access: Option<AccessMode>) -> Result<()> { self.ensure_open()?; self.default_access_mode = access; Ok(()) }
    pub fn set_parameter(&mut self, name: impl Into<String>, value: Option<String>) -> Result<()> { self.ensure_open()?; self.parameters.insert(name.into(), value); Ok(()) }
    pub fn parameters(&self) -> BTreeMap<String, Option<String>> {
        let mut parameters = self.parameters.clone();
        parameters.insert("timezone".into(), Some(self.effective_timezone().into()));
        parameters
    }
    pub fn begin(&mut self) -> Result<()> {
        self.ensure_open()?;
        if self.transaction().is_some() { return Err(Anomaly::incorrect("xtdb/tx-already-open", "transaction already started")); }
        let phase = match self.default_access_mode {
            Some(AccessMode::ReadOnly) => Phase::Pending(Some(ReadOptions::default())),
            Some(AccessMode::ReadWrite) => Phase::Writing { buffer: DmlBuffer::default(), options: WriteOptions::default() },
            None => Phase::Pending(None),
        };
        self.lifecycle = Lifecycle::Open(Some(Transaction::new(phase, &self.timezone)));
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
        let mut created = Transaction::new(Phase::Pending(None), &self.timezone);
        let tx = self.transaction_mut().unwrap_or(&mut created);
        let timezone = rtdb_expr::normalize_timezone(&settings.timezone.apply(&Some(tx.timezone.clone())).unwrap_or_else(|| tx.session_timezone.clone()))?;
        let next = match &settings.access {
            None => None,
            Some(AccessSettings::ReadOnly(settings)) => {
                let old = match &tx.phase { Phase::Pending(Some(options)) => options.clone(), _ => ReadOptions::default() };
                let options = ReadOptions { snapshot_token: settings.snapshot_token.apply(&old.snapshot_token), snapshot_time: settings.snapshot_time.apply(&old.snapshot_time),
                    clock_time: settings.clock_time.apply(&old.clock_time), await_token: settings.await_token.apply(&old.await_token) };
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
        if let Some(tx) = self.transaction_mut() {
            if matches!(&tx.phase, Phase::Writing { buffer, .. } if !buffer.is_empty()) {
                return Err(Anomaly::incorrect("xtdb/set-tz-in-write-tx", "Cannot SET TIME ZONE in a read-write transaction with buffered writes"));
            }
            tx.timezone = timezone.clone(); tx.session_timezone = timezone;
        } else { self.timezone = timezone; }
        Ok(())
    }
    pub fn fail_transaction(&mut self, error: Anomaly) { if let Some(tx) = self.transaction_mut() { if tx.failure.is_none() { tx.failure = Some(error); } } }
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
        Ok(QueryBasis { snapshot, snapshot_time: options.snapshot_time, current_time: options.clock_time.unwrap_or_else(|| self.service.now()), timezone: self.effective_timezone().to_owned() })
    }
    pub async fn read_basis(&mut self) -> Result<QueryBasis> {
        self.revalidate().await?;
        self.require_usable_transaction()?;
        if let Some(tx) = self.transaction() {
            match &tx.phase {
                Phase::Writing { .. } => return Err(Anomaly::incorrect("xtdb/queries-in-read-write-tx", "Queries are unsupported in a DML transaction")),
                Phase::Reading { basis, .. } => { let mut basis = basis.clone(); basis.timezone = tx.timezone.clone(); return Ok(basis); }
                Phase::Pending(_) => (),
            }
        }
        let options = match self.transaction().map(|tx| &tx.phase) { Some(Phase::Pending(Some(options))) => options.clone(), _ => ReadOptions::default() };
        let basis = self.pin(&options).await?;
        if let Some(tx) = self.transaction_mut() { tx.phase = Phase::Reading { basis: basis.clone(), await_token: options.await_token }; }
        else if !self.auto_commit { self.lifecycle = Lifecycle::Open(Some(Transaction::new(Phase::Reading { basis: basis.clone(), await_token: None }, &self.timezone))); }
        Ok(basis)
    }
    pub async fn execute_operation(&mut self, operation: TxOp) -> Result<Option<TransactionResult>> {
        self.revalidate().await?;
        self.require_usable_transaction()?;
        if self.auto_commit && self.transaction().is_none() {
            let options = SubmitOptions { default_tz: self.timezone.clone(), ..SubmitOptions::default() };
            let result = self.submit_operations(vec![operation], options, CommitMode::Sync).await?;
            let CommitResult::Executed(result) = result else { return Err(Anomaly::fault("rtdb.node/commit-mode", "Synchronous submission returned no transaction result")); };
            if let Some(error) = result.error() { return Err(error.clone()); }
            return Ok(Some(result));
        }
        if self.transaction().is_none() { self.lifecycle = Lifecycle::Open(Some(Transaction::new(Phase::Pending(None), &self.timezone))); }
        let tx = self.transaction_mut().ok_or_else(|| Anomaly::fault("rtdb.node/transaction-state", "Buffered operation has no open transaction"))?;
        if matches!(&tx.phase, Phase::Pending(None)) { tx.phase = Phase::Writing { buffer: DmlBuffer::default(), options: WriteOptions::default() }; }
        match &mut tx.phase {
            Phase::Writing { buffer, .. } => buffer.append(operation)?,
            _ => return Err(Anomaly::incorrect("xtdb/read-only-tx", "Cannot write in a read-only transaction")),
        }
        Ok(None)
    }
    async fn submit_operations(&mut self, operations: Vec<TxOp>, options: SubmitOptions, mode: CommitMode) -> Result<CommitResult> {
        self.submit_to(self.database.clone(), operations, options, mode).await
    }
    async fn submit_to(&mut self, database: String, operations: Vec<TxOp>, mut options: SubmitOptions, mode: CommitMode) -> Result<CommitResult> {
        self.revalidate().await?;
        self.ensure_open()?;
        options.user = self.service.user().map(str::to_owned);
        if options.default_tz.is_empty() { options.default_tz = self.timezone.clone(); }
        let submitted = self.service.submit(&database, operations, options, &self.cancellation).await?;
        let next = TxBasis { databases: BTreeMap::from([(database.clone(), vec![submitted.tx_id])]) };
        let token = match &self.await_token { Some(token) => decode_tx_basis(token)?.merge(&next)?, None => next };
        match mode {
            CommitMode::Async => { self.await_token = Some(encode_tx_basis(&token)); self.last_submitted = Some(LastSubmittedTx::Submitted(submitted)); Ok(CommitResult::Submitted(submitted)) }
            CommitMode::Sync => {
                let result = self.service.await_transaction(&database, submitted.tx_id, &self.cancellation).await?;
                self.await_token = Some(encode_tx_basis(&token)); self.last_submitted = Some(LastSubmittedTx::Executed(result.clone()));
                Ok(CommitResult::Executed(result))
            }
        }
    }
    async fn autonomous(&mut self, operations: Vec<TxOp>, options: AutonomousTxOptions, mode: CommitMode) -> Result<CommitResult> {
        self.ensure_open()?;
        if self.transaction().is_some() { return Err(Anomaly::incorrect("xtdb/tx-already-in-progress", "Cannot submit a transaction while another is in progress on this connection - commit or roll back first")); }
        let timezone = rtdb_expr::normalize_timezone(options.default_timezone.as_deref().unwrap_or(&self.timezone))?;
        self.submit_to(options.database.unwrap_or_else(|| self.database.clone()), operations,
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
        self.submit_operations(buffer.operations, SubmitOptions { default_tz: tx.timezone, system_time: options.system_time,
            user: None, user_metadata: options.user_metadata }, mode).await
    }
    pub async fn commit(&mut self) -> Result<CommitResult> {
        if self.auto_commit { return Err(Anomaly::incorrect("xtdb.adbc/commit-in-autocommit", "Cannot commit when autoCommit is enabled")); }
        let result = self.commit_tx(None).await?;
        if let CommitResult::Executed(tx) = &result { if let Some(error) = tx.error() { return Err(error.clone()); } }
        Ok(result)
    }
    pub fn close(&mut self) { self.cancellation.cancel(); self.lifecycle = Lifecycle::Closed; }
}

impl Drop for Session { fn drop(&mut self) { self.cancellation.cancel(); } }
