use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use parking_lot::Mutex;
use rtdb_columnar::MemoryBudget;
use rtdb_format::basis::{decode_tx_basis, encode_tx_basis};
use rtdb_node::*;
use rtdb_plan::AccessMode;
use rtdb_tx::{SqlArguments, SubmitOptions, SubmittedTx, TransactionResult, TxOp};
use rtdb_types::{Anomaly, Instant, Result, TimeBasis, Timestamp, TransactionKey, TxBasis, TxId, Value};
use tokio_util::sync::CancellationToken;

struct State { clock: i64, extra_nanos: u32, submissions: Vec<(String, Vec<TxOp>, SubmitOptions)>, observations: Vec<&'static str>, abort: Option<Anomaly> }
#[derive(Default)]
struct Metrics { durations: Mutex<Vec<rtdb_plan::ExecutionTimer>>, errors: Mutex<Vec<rtdb_plan::ExecutionError>> }
impl rtdb_plan::ExecutionObserver for Metrics {
    fn record_duration(&self, timer: rtdb_plan::ExecutionTimer, _: Duration) { self.durations.lock().push(timer); }
    fn record_error(&self, error: rtdb_plan::ExecutionError) { self.errors.lock().push(error); }
}
struct Service { state: Mutex<State>, memory: Arc<MemoryBudget>, metrics: Arc<Metrics> }
impl Service {
    fn new() -> Arc<Self> { Self::with_memory(1_000_000) }
    fn with_memory(bytes: usize) -> Arc<Self> { Arc::new(Self { state: Mutex::new(State { clock: 1, extra_nanos: 0, submissions: Vec::new(), observations: Vec::new(), abort: None }), memory: MemoryBudget::new(bytes), metrics: Arc::new(Metrics::default()) }) }
    fn session(self: &Arc<Self>) -> Session { Session::new(self.clone(), "xtdb", "UTC", &CancellationToken::new()).unwrap() }
}
#[async_trait]
impl SessionService for Service {
    fn memory(&self) -> &Arc<MemoryBudget> { &self.memory }
    fn execution_observer(&self) -> Option<Arc<dyn rtdb_plan::ExecutionObserver>> { Some(self.metrics.clone()) }
    fn user(&self) -> Option<&str> { Some("alice") }
    fn check_open(&self) -> Result<()> { Ok(()) }
    async fn revalidate(&self) -> Result<()> { Ok(()) }
    fn now(&self) -> Timestamp { Timestamp(self.state.lock().clock) }
    fn query_instant(&self) -> Instant {
        let state = self.state.lock();let base = Instant::from(Timestamp(state.clock));
        Instant::new(base.seconds(),base.nanos()+state.extra_nanos).unwrap()
    }
    fn snapshot_basis(&self) -> Result<TimeBasis> {
        let mut state = self.state.lock();
        state.observations.push("snapshot");
        Ok(TimeBasis { databases: BTreeMap::from([("xtdb".into(), vec![Some(Instant::new(state.clock, 0).unwrap())])]) })
    }
    async fn await_basis(&self, _: Option<TxBasis>, _: Duration, _: &CancellationToken) -> Result<()> { self.state.lock().observations.push("await-basis"); Ok(()) }
    async fn submit(&self, database: &str, operations: Vec<TxOp>, options: SubmitOptions, _: &CancellationToken) -> Result<SubmittedTx> {
        let mut state = self.state.lock();
        state.observations.push("submit");
        state.submissions.push((database.into(), operations, options));
        Ok(SubmittedTx { tx_id: TxId(state.submissions.len() as i64), placed_receipt: None })
    }
    async fn await_transaction(&self, _: &str, tx_id: TxId, _: &CancellationToken) -> Result<TransactionResult> {
        let mut state = self.state.lock(); state.observations.push("apply");
        let tx_key = TransactionKey { tx_id, system_time: Timestamp(state.clock) };
        Ok(match &state.abort { Some(error) => TransactionResult::Aborted { tx_key, error: Some(rtdb_tx::RetainedAnomaly::new(error.clone(), &rtdb_columnar::MemoryBudget::new(1_000_000)).unwrap()) }, None => TransactionResult::Committed { tx_key } })
    }
}
fn operation() -> TxOp { TxOp::Sql { sql: "INSERT INTO foo (_id) VALUES (1)".into(), args: None } }
fn read_only() -> TransactionSettings { TransactionSettings { access: Some(AccessSettings::ReadOnly(ReadOnlySettings::default())), ..Default::default() } }
fn read_write() -> TransactionSettings { TransactionSettings { access: Some(AccessSettings::ReadWrite(ReadWriteSettings::default())), ..Default::default() } }

#[tokio::test]
async fn show_basis_after_async_submit_does_not_await_or_resolve_pending_read_only() {
    let service = Service::new(); let mut session = service.session();
    session.submit_tx(vec![operation()], Default::default()).await.unwrap();
    session.begin_with(read_only()).unwrap();
    let first = session.inspection_basis().unwrap();
    assert_eq!(session.status(), TransactionStatus::Pending);
    assert_eq!(service.state.lock().observations, ["submit", "snapshot"]);
    service.state.lock().clock = 2;
    assert_ne!(session.inspection_basis().unwrap(), first);
    let pinned = session.read_basis().await.unwrap();
    service.state.lock().clock = 3;
    assert_eq!(session.inspection_basis().unwrap(), pinned);
    assert_eq!(service.state.lock().observations, ["submit", "snapshot", "snapshot", "await-basis", "snapshot"]);
}

#[tokio::test]
async fn connection_transaction_metrics_distinguish_async_submission_sync_wait_and_abort() {
    use rtdb_plan::{ExecutionError, ExecutionTimer};
    let service = Service::new(); let mut session = service.session();
    session.submit_tx(vec![operation()], Default::default()).await.unwrap();
    assert_eq!(*service.metrics.durations.lock(), [ExecutionTimer::TxSubmit]);
    service.state.lock().abort = Some(Anomaly::incorrect("test/abort", "aborted"));
    assert!(!session.execute_tx(vec![operation()], Default::default()).await.unwrap().committed());
    assert_eq!(*service.metrics.durations.lock(), [ExecutionTimer::TxSubmit, ExecutionTimer::TxAwait, ExecutionTimer::TxExecute]);
    assert_eq!(*service.metrics.errors.lock(), [ExecutionError::Transaction]);
    session.begin().unwrap(); session.execute_operation(operation()).await.unwrap();
    session.rollback_tx().unwrap();
    assert_eq!(service.metrics.durations.lock().len(), 3);
}

#[tokio::test]
async fn prepare_and_unchecked_reads_leave_unspecified_pending_transactions_unresolved() {
    let service = Service::new(); let mut session = service.session(); session.begin().unwrap();
    let prepared = session.prepare_basis().await.unwrap();
    assert_eq!(session.status(), TransactionStatus::Pending);
    service.state.lock().clock = 2;
    let unchecked = session.unchecked_read_basis().await.unwrap();
    assert_ne!(prepared, unchecked);
    assert_eq!(session.status(), TransactionStatus::Pending);
    session.execute_operation(operation()).await.unwrap();
    assert_eq!(session.status(), TransactionStatus::Writing);
    assert!(session.unchecked_read_basis().await.is_ok());
    assert_eq!(session.read_basis().await.unwrap_err().code, "xtdb/queries-in-read-write-tx");
}

#[tokio::test]
async fn unchecked_reads_pin_declared_read_only_but_preparation_does_not() {
    let service = Service::new(); let mut session = service.session(); session.begin_with(read_only()).unwrap();
    session.prepare_basis().await.unwrap();
    assert_eq!(session.status(), TransactionStatus::Pending);
    let first = session.unchecked_read_basis().await.unwrap();
    assert_eq!(session.status(), TransactionStatus::Reading);
    service.state.lock().clock = 10;
    assert_eq!(first, session.unchecked_read_basis().await.unwrap());
}

#[tokio::test]
async fn autonomous_transactions_use_the_target_database_and_keep_the_connection_database() {
    let service = Service::new();
    let mut session = service.session();
    session.set_timezone("Europe/Paris").unwrap();
    let tx = session.submit_tx(vec![operation()], AutonomousTxOptions { database: Some("other".into()), ..Default::default() }).await.unwrap();
    assert_eq!(session.database(), "xtdb");
    let basis = decode_tx_basis(session.effective_await_token().unwrap()).unwrap();
    assert_eq!(basis.databases, BTreeMap::from([("other".into(), vec![tx.tx_id])]));
    let state = service.state.lock();
    assert_eq!(state.submissions[0].0, "other");
    assert_eq!(state.submissions[0].2.default_tz, "Europe/Paris");
    assert_eq!(state.submissions[0].2.user.as_deref(), Some("alice"));
}

#[tokio::test]
async fn autonomous_execution_returns_an_abort_and_rejects_an_open_transaction() {
    let service = Service::new();
    service.state.lock().abort = Some(Anomaly::incorrect("test/abort", "aborted"));
    let mut session = service.session();
    let result = session.execute_tx(vec![operation()], AutonomousTxOptions::default()).await.unwrap();
    assert!(!result.committed());
    assert_eq!(result.error().unwrap().code, "test/abort");
    session.begin().unwrap();
    assert_eq!(session.submit_tx(vec![operation()], AutonomousTxOptions::default()).await.unwrap_err().code, "xtdb/tx-already-in-progress");
    assert_eq!(service.state.lock().submissions.len(), 1);
}

#[tokio::test]
async fn a_bare_begin_pins_only_on_first_query_after_awaiting_its_basis() {
    let service = Service::new(); let mut session = service.session();
    session.begin().unwrap();
    assert_eq!(session.status(), TransactionStatus::Pending);
    assert!(service.state.lock().observations.is_empty());
    service.state.lock().clock = 2;
    let first = session.read_basis().await.unwrap();
    service.state.lock().clock = 3;
    assert_eq!(session.read_basis().await.unwrap(), first);
    assert_eq!(service.state.lock().observations, vec!["await-basis", "snapshot"]);
    assert_eq!(session.execute_operation(operation()).await.unwrap_err().code, "xtdb/read-only-tx");
    session.commit_tx(None).await.unwrap();
    assert_ne!(session.read_basis().await.unwrap(), first);
}

#[tokio::test]
async fn manual_mode_lazily_pins_each_transaction_but_autocommit_reads_are_fresh() {
    let service = Service::new(); let mut session = service.session();
    session.set_auto_commit(false).await.unwrap();
    assert_eq!(session.status(), TransactionStatus::Idle);
    let first = session.read_basis().await.unwrap(); service.state.lock().clock = 2;
    assert_eq!(session.read_basis().await.unwrap(), first);
    session.rollback().unwrap(); session.set_auto_commit(true).await.unwrap();
    let second = session.read_basis().await.unwrap(); service.state.lock().clock = 3;
    assert_ne!(session.read_basis().await.unwrap(), second);
    assert_eq!(session.status(), TransactionStatus::Idle);
}

#[tokio::test]
async fn explicit_begin_buffers_even_when_autocommit_is_enabled() {
    let service = Service::new(); let mut session = service.session();
    session.begin().unwrap(); session.execute_operation(operation()).await.unwrap();
    assert!(session.auto_commit()); assert!(service.state.lock().submissions.is_empty());
    assert_eq!(session.read_basis().await.unwrap_err().code, "xtdb/queries-in-read-write-tx");
    session.commit_tx(None).await.unwrap();
    assert_eq!(service.state.lock().submissions.len(), 1);
    assert_eq!(service.state.lock().observations, vec!["submit", "apply"]);
}

#[tokio::test]
async fn empty_writing_commit_submits_but_pending_and_reading_commit_do_not() {
    let service = Service::new(); let mut session = service.session();
    session.begin().unwrap(); session.commit_tx(None).await.unwrap();
    session.begin_with(read_only()).unwrap(); session.read_basis().await.unwrap(); session.commit_tx(None).await.unwrap();
    assert!(service.state.lock().submissions.is_empty());
    session.begin_with(read_write()).unwrap(); session.commit_tx(None).await.unwrap();
    let state = service.state.lock(); assert_eq!(state.submissions.len(), 1); assert!(state.submissions[0].1.is_empty());
}

#[tokio::test]
async fn consecutive_sql_argument_batches_coalesce_without_collapsing_an_absent_batch() {
    let service = Service::new(); let mut session = service.session(); let memory = MemoryBudget::new(1 << 20);
    let sql = "INSERT INTO foo (_id) VALUES (?)".to_owned();
    session.begin().unwrap();
    for value in [1, 2] {
        let args = SqlArguments::from_rows(vec![vec![Value::I64(value)]], &memory).unwrap();
        session.execute_operation(TxOp::Sql { sql: sql.clone(), args: Some(args) }).await.unwrap();
    }
    session.execute_operation(TxOp::Sql { sql: sql.clone(), args: None }).await.unwrap();
    session.execute_operation(TxOp::Sql { sql, args: Some(SqlArguments::new(vec![rtdb_types::LogicalType::Scalar(rtdb_types::ScalarType::I64)], vec![], &memory).unwrap()) }).await.unwrap();
    session.commit_tx(None).await.unwrap();
    let state = service.state.lock(); let ops = &state.submissions[0].1;
    assert_eq!(ops.len(), 3);
    let TxOp::Sql { args: Some(args), .. } = &ops[0] else { panic!("coalesced arguments absent"); };
    assert_eq!(args.rows().map(|row| row.unwrap().to_vec()).collect::<Vec<_>>(), vec![&[Value::I64(1)][..], &[Value::I64(2)][..]]);
    let TxOp::Sql { args: Some(args), .. } = &ops[2] else { panic!("empty arguments absent"); };
    assert_eq!(args.row_count(), 0);
}

#[tokio::test]
async fn session_timezone_commits_while_transaction_timezone_reverts() {
    let service = Service::new(); let mut session = service.session();
    session.begin_with(TransactionSettings { timezone: OptionUpdate::Set("Europe/Paris".into()), ..read_write() }).unwrap();
    session.execute_operation(operation()).await.unwrap(); session.commit_tx(None).await.unwrap();
    assert_eq!(service.state.lock().submissions[0].2.default_tz, "Europe/Paris");
    assert_eq!(session.effective_timezone(), "UTC");
    session.begin().unwrap(); session.set_timezone("America/New_York").unwrap(); session.commit_tx(None).await.unwrap();
    assert_eq!(session.effective_timezone(), "America/New_York");
    session.begin().unwrap(); session.set_timezone("Europe/Paris").unwrap(); session.rollback_tx().unwrap();
    assert_eq!(session.effective_timezone(), "America/New_York");
    session.execute_operation(operation()).await.unwrap();
    assert_eq!(service.state.lock().submissions.last().unwrap().2.default_tz, "America/New_York");
}

#[tokio::test]
async fn options_can_be_cleared_before_the_first_statement_and_begin_failure_rolls_back() {
    let service = Service::new(); let mut session = service.session();
    let invalid = TransactionSettings { access: Some(AccessSettings::ReadOnly(ReadOnlySettings { await_token: OptionUpdate::Set("not-base64".into()), ..Default::default() })), ..Default::default() };
    assert!(session.begin_with(invalid).is_err()); assert_eq!(session.status(), TransactionStatus::Idle);
    session.begin_with(TransactionSettings { access: Some(AccessSettings::ReadOnly(ReadOnlySettings { clock_time: OptionUpdate::Set(Timestamp(99).into()), ..Default::default() })), ..Default::default() }).unwrap();
    session.set_transaction(TransactionSettings { access: Some(AccessSettings::ReadOnly(ReadOnlySettings { clock_time: OptionUpdate::Clear, ..Default::default() })), ..Default::default() }).unwrap();
    assert_eq!(session.read_basis().await.unwrap().current_time, Timestamp(1).into());
    assert_eq!(session.set_transaction(read_write()).unwrap_err().code, "xtdb/set-transaction-after-first-statement");
    session.set_transaction(TransactionSettings::default()).unwrap();
}

#[tokio::test]
async fn awaited_writes_merge_tokens_across_database_changes_and_capture_identity() {
    let service = Service::new(); let mut session = service.session();
    session.execute_operation(operation()).await.unwrap(); session.set_database("other").unwrap(); session.execute_operation(operation()).await.unwrap();
    let token = decode_tx_basis(session.effective_await_token().unwrap()).unwrap();
    assert_eq!(token.databases, BTreeMap::from([("xtdb".into(), vec![TxId(1)]), ("other".into(), vec![TxId(2)])]));
    assert_eq!(session.last_submitted().unwrap().tx_id(), TxId(2));
    assert!(service.state.lock().submissions.iter().all(|(_, _, options)| options.user.as_deref() == Some("alice")));
}

#[tokio::test]
async fn read_only_await_override_shadows_connection_token_only_within_transaction() {
    let service = Service::new(); let mut session = service.session();
    let token = encode_tx_basis(&TxBasis { databases: BTreeMap::from([("xtdb".into(), vec![TxId(8)])]) });
    let local = encode_tx_basis(&TxBasis { databases: BTreeMap::from([("xtdb".into(), vec![TxId(9)])]) });
    session.set_await_token(Some(token.clone())).unwrap();
    session.begin_with(TransactionSettings { access: Some(AccessSettings::ReadOnly(ReadOnlySettings { await_token: OptionUpdate::Set(local.clone()), ..Default::default() })), ..Default::default() }).unwrap();
    assert_eq!(session.effective_await_token(), Some(local.as_str())); session.read_basis().await.unwrap();
    assert_eq!(session.effective_await_token(), Some(local.as_str())); session.rollback_tx().unwrap();
    assert_eq!(session.effective_await_token(), Some(token.as_str()));
}

#[tokio::test]
async fn commit_override_selects_submission_or_applied_completion() {
    let service = Service::new(); let mut session = service.session();
    let settings = TransactionSettings { access: Some(AccessSettings::ReadWrite(ReadWriteSettings { asynchronous: OptionUpdate::Set(true), ..Default::default() })), ..Default::default() };
    session.begin_with(settings.clone()).unwrap(); session.commit_tx(Some(CommitMode::Sync)).await.unwrap();
    assert_eq!(service.state.lock().observations, vec!["submit", "apply"]);
    session.begin_with(settings).unwrap(); session.commit_tx(None).await.unwrap();
    assert_eq!(service.state.lock().observations, vec!["submit", "apply", "submit"]);
    assert!(matches!(session.last_submitted(), Some(LastSubmittedTx::Submitted(_))));
}

#[tokio::test]
async fn failed_transaction_rejects_work_until_rollback_and_close_discards_buffer() {
    let service = Service::new(); let mut session = service.session();
    session.begin().unwrap(); session.execute_operation(operation()).await.unwrap();
    session.fail_transaction();
    assert_eq!(session.read_basis().await.unwrap_err().code, "xtdb/tx-failed");
    assert_eq!(session.execute_operation(operation()).await.unwrap_err().code, "xtdb/tx-failed");
    session.rollback_tx().unwrap(); session.begin().unwrap(); session.execute_operation(operation()).await.unwrap(); session.close();
    assert!(service.state.lock().submissions.is_empty()); assert_eq!(session.status(), TransactionStatus::Closed);
}

#[tokio::test]
async fn manual_commit_errors_and_session_defaults_follow_connection_contract() {
    let service = Service::new(); let mut session = service.session();
    assert_eq!(session.commit().await.unwrap_err().code, "xtdb.adbc/commit-in-autocommit");
    assert_eq!(session.rollback().unwrap_err().code, "xtdb.adbc/rollback-in-autocommit");
    session.set_session_characteristics(Some(AccessMode::ReadOnly)).unwrap(); session.begin().unwrap();
    assert!(session.is_read_only()); session.rollback_tx().unwrap(); session.reset().unwrap();
    session.set_auto_commit(false).await.unwrap(); session.execute_operation(operation()).await.unwrap();
    session.set_auto_commit(true).await.unwrap(); assert_eq!(service.state.lock().submissions.len(), 1);
}

#[test]
fn session_timezone_echo_normalizes_fixed_offsets_and_zero() {
    let service = Service::new(); let mut session = service.session();
    session.set_timezone("UTC+1").unwrap();
    assert_eq!(session.parameters().get("timezone"), Some(&Some("UTC+01:00".into())));
    session.set_timezone("-00:00").unwrap();
    assert_eq!(session.effective_timezone(), "Z");
}

#[test]
fn retained_parameters_reject_over_budget_updates_atomically() {
    let service = Service::with_memory(4096);
    let mut session = service.session();
    let baseline = service.memory.used_bytes();
    session.set_parameter("application_name", Some("client".into())).unwrap();
    let retained = service.memory.used_bytes();
    assert!(retained > baseline);
    assert!(session.set_parameter("application_name", Some("x".repeat(4096))).is_err());
    assert_eq!(session.parameter("application_name"), Some("client"));
    assert_eq!(service.memory.used_bytes(), retained);
    session.reset().unwrap();
    assert_eq!(session.application_name(), "client");
    assert_eq!(service.memory.used_bytes(), retained);
    drop(session);
    assert_eq!(service.memory.used_bytes(), 0);
}

#[test]
fn marking_a_transaction_failed_preserves_its_memory_charge() {
    let service = Service::with_memory(4096);
    let mut session = service.session();
    session.begin().unwrap();
    let before = service.memory.used_bytes();
    session.fail_transaction();
    assert_eq!(session.status(), TransactionStatus::Failed);
    assert_eq!(service.memory.used_bytes(), before);
    session.rollback_tx().unwrap();
    assert_eq!(session.status(), TransactionStatus::Idle);
}

#[tokio::test]
async fn buffered_sql_owns_its_charge_until_rollback_or_commit() {
    let service = Service::with_memory(8192);
    let mut session = service.session();
    session.set_auto_commit(false).await.unwrap();
    let baseline = service.memory.used_bytes();
    session.execute_operation(TxOp::Sql { sql: "x".repeat(2048), args: None }).await.unwrap();
    assert!(service.memory.used_bytes() >= baseline + 2048);
    assert!(session.execute_operation(TxOp::Sql { sql: "x".repeat(8192), args: None }).await.is_err());
    session.rollback_tx().unwrap();
    assert_eq!(service.memory.used_bytes(), baseline);
    session.execute_operation(operation()).await.unwrap();
    session.commit_tx(None).await.unwrap();
    assert!(session.effective_await_token().is_some());
    session.close();
    assert_eq!(service.memory.used_bytes(), baseline);
    drop(session);
    assert_eq!(service.memory.used_bytes(), 0);
}

#[tokio::test]
async fn read_basis_and_transaction_tokens_release_on_rollback() {
    let service = Service::with_memory(8192);
    let mut session = service.session();
    let baseline = service.memory.used_bytes();
    session.begin_with(TransactionSettings { access: Some(AccessSettings::ReadOnly(ReadOnlySettings {
        snapshot_token: OptionUpdate::Set(rtdb_format::basis::encode_time_basis(&TimeBasis::default())), ..Default::default()
    })), ..Default::default() }).unwrap();
    assert!(service.memory.used_bytes() > baseline);
    session.read_basis().await.unwrap();
    assert!(service.memory.used_bytes() > baseline);
    session.rollback_tx().unwrap();
    assert_eq!(service.memory.used_bytes(), baseline);
}

#[tokio::test]
async fn pinned_read_basis_retains_submicrosecond_clock_and_commit_time_stays_microseconds() {
    let service = Service::new();let mut session = service.session();
    let clock = Instant::new(0,999).unwrap();
    session.begin_with(TransactionSettings { access: Some(AccessSettings::ReadOnly(ReadOnlySettings { clock_time: OptionUpdate::Set(clock),..Default::default() })),..Default::default() }).unwrap();
    let basis = session.read_basis().await.unwrap();
    assert_eq!(basis.current_time,clock);
    service.state.lock().clock = 7;
    assert_eq!(session.read_basis().await.unwrap(),basis);
    session.rollback_tx().unwrap();
    let result = session.submit_tx(vec![operation()],AutonomousTxOptions { system_time: Some(Timestamp(2)),..Default::default() }).await.unwrap();
    assert_eq!(result.tx_id,TxId(1));
    assert_eq!(service.state.lock().submissions[0].2.system_time,Some(Timestamp(2)));
}

#[tokio::test]
async fn session_clock_capability_keeps_exact_readings_until_the_basis_is_pinned() {
    let service = Service::new();service.state.lock().extra_nanos = 789;
    let mut session = service.session();
    assert_eq!(session.read_basis().await.unwrap().current_time,Instant::new(0,1789).unwrap());
    session.begin_with(read_only()).unwrap();
    let pinned = session.read_basis().await.unwrap();
    service.state.lock().extra_nanos = 999;
    assert_eq!(session.read_basis().await.unwrap(),pinned);
    session.rollback_tx().unwrap();
    assert_eq!(session.read_basis().await.unwrap().current_time,Instant::new(0,1999).unwrap());
}
