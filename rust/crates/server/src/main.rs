use std::{io::Write, process::ExitCode, time::Duration};
use rtdb_server::{ServerDriver, ServerHandle, cli::{self, Command}};
use rtdb_types::{Anomaly, Result};

fn main() -> ExitCode {
    let runtime = match tokio::runtime::Builder::new_multi_thread().thread_stack_size(rtdb_node::WORKER_STACK_BYTES).enable_all().build() {
        Ok(runtime) => runtime,
        Err(error) => { eprintln!("{error}"); return ExitCode::FAILURE; }
    };
    let result = runtime.block_on(run());
    runtime.shutdown_timeout(Duration::ZERO);
    match result {
        Ok(()) => ExitCode::SUCCESS,
        Err(error) => { eprintln!("{error}"); ExitCode::from(if error.code == "rtdb.cli/arguments" { 2 } else { 1 }) }
    }
}

async fn run() -> Result<()> {
    let command = cli::parse(std::env::args().skip(1), &|key| std::env::var(key).ok())?;
    match command {
        Command::Help => { print!("{}", cli::HELP); Ok(()) }
        Command::Version => { println!("rtdb {}", rtdb_types::runtime_version()); Ok(()) }
        Command::BuildInfo => write_report(std::io::stdout().lock(), |output| {
            serde_json::to_writer(&mut *output, &rtdb_types::compiled_build_info()).map_err(std::io::Error::other)?;
            writeln!(output)
        }),
        Command::HashPassword { bcrypt, password } => { println!("{}", cli::hash_password(bcrypt, password, &mut std::io::stdin().lock())?); Ok(()) }
        Command::Inspect { kind, path, allocation_limit_bytes } => rtdb_server::inspect::file(kind, &path, &rtdb_columnar::MemoryBudget::new(allocation_limit_bytes), &mut std::io::stdout().lock()),
        Command::SnapshotExport { options, database, dry_run } => export_snapshot(options, database, dry_run).await,
        Command::CompactorReset { options, database, dry_run, retained_base } => reset_compactor(options, database, dry_run, retained_base).await,
        Command::RetainRepairBase { request, memory_bytes } => {
            let memory = rtdb_columnar::MemoryBudget::new(memory_bytes);
            let cancellation = tokio_util::sync::CancellationToken::new();
            let installation = request.install(memory, &cancellation);
            tokio::pin!(installation);
            let boundary = tokio::select! {
                result = &mut installation => result?,
                _ = shutdown_signal() => { cancellation.cancel(); installation.await? }
            };
            write_report(std::io::stdout().lock(), |output| {
                writeln!(output, "Retained repair base {} from replica boundary {}", request.base.block.0, boundary.0)
            })
        },
        Command::RepairCheckpoint { request, memory_bytes } => {
            let memory = rtdb_columnar::MemoryBudget::new(memory_bytes);
            let cancellation = tokio_util::sync::CancellationToken::new();
            report_checkpoint_repair(request.rebuild(memory, &cancellation), &cancellation).await
        },
        Command::RepairRetainedCheckpoint { request, memory_bytes } => {
            let memory = rtdb_columnar::MemoryBudget::new(memory_bytes);
            let cancellation = tokio_util::sync::CancellationToken::new();
            report_checkpoint_repair(request.rebuild(memory, &cancellation), &cancellation).await
        },
        Command::Start(options) => {
            let (config, memory, _configuration) = options.load_reserved()?.into_execution(options.allocation_limit_bytes)?;
            let (handle, driver) = if options.mode == cli::NodeMode::Compactor {
                ServerDriver::open_compactor_with_memory(config, options.allocation_limit_bytes, memory).await?
            } else { ServerDriver::open_with_memory(config, options.allocation_limit_bytes, memory).await? };
            let announced = write_report(std::io::stderr().lock(), |output| writeln!(output, "RTDB listeners bound: {:?}", handle.addresses()));
            if let Err(error) = announced { return close_unstarted(driver, Err(error)).await; }
            run_server(&handle, driver, shutdown_signal()).await
        }
    }
}

async fn report_checkpoint_repair(
    repair: impl std::future::Future<Output = Result<rtdb_node::RepairedStore>>,
    cancellation: &tokio_util::sync::CancellationToken,
) -> Result<()> {
    tokio::pin!(repair);
    let store = tokio::select! {
        result = &mut repair => result?,
        _ = shutdown_signal() => { cancellation.cancel(); repair.await? }
    };
    write_report(std::io::stdout().lock(), |output| {
        let digest: String = store.checkpoint.sha256.iter().map(|byte| format!("{byte:02x}")).collect();
        writeln!(output, "Rebuilt checkpoint {}@{digest} through transaction {}", store.checkpoint.block.0, store.latest_transaction.tx_id.0)?;
        writeln!(output, "Storage: {}", store.path.display())?;
        writeln!(output, "Source replica epoch: {}", store.source_replica_epoch.0)
    })
}

async fn export_snapshot(options: cli::StartOptions, database: String, dry_run: bool) -> Result<()> {
    let (mut config, memory, _configuration) = options.load_reserved()?.into_execution(options.allocation_limit_bytes)?;
    config.compactor.threads = 0;
    config.indexer.enabled = false;
    config.tracing.enabled = false;
    config.server = None;
    config.flight_sql = None;
    config.health = None;
    let (handle, driver) = ServerDriver::open_with_memory(config, options.allocation_limit_bytes, memory).await?;
    let plan = match handle.node().snapshot_export(&database, handle.node().now()).await {
        Ok(Some(plan)) => plan,
        Ok(None) => {
            let reported = write_report(std::io::stderr().lock(), |output| writeln!(output, "No completed blocks found - aborting."));
            return close_unstarted(driver, reported).await;
        }
        Err(error) => return close_unstarted(driver, Err(error)).await,
    };
    let announced = write_report(std::io::stderr().lock(), |output| writeln!(output, "Export directory: {}", plan.directory()));
    if let Err(error) = announced { return close_unstarted(driver, Err(error)).await; }
    if dry_run {
        let reported = write_report(std::io::stdout().lock(), |output| {
            writeln!(output, "DRY RUN: No files will be copied\nFiles that would be copied:")?;
            for key in plan.files() { writeln!(output, "{key}")?; }
            Ok(())
        });
        return close_unstarted(driver, reported).await;
    }
    let cancellation = tokio_util::sync::CancellationToken::new();
    run_operation(&handle, driver, &cancellation, plan.copy(&cancellation)).await?;
    write_report(std::io::stderr().lock(), |output| writeln!(output, "Export complete: copied {} files to {}", plan.files().len(), plan.directory()))
}

async fn reset_compactor(options: cli::StartOptions, database: String, dry_run: bool, retained_base: Option<std::path::PathBuf>) -> Result<()> {
    let (mut config, memory, _configuration) = options.load_reserved()?.into_execution(options.allocation_limit_bytes)?;
    config.compactor.threads = 0;
    config.indexer.enabled = true;
    config.garbage.enabled = false;
    config.server = None;
    config.flight_sql = None;
    config.health = None;
    let (handle, driver) = ServerDriver::open_with_memory(config, options.allocation_limit_bytes, memory).await?;
    let cancellation = tokio_util::sync::CancellationToken::new();
    let announced = write_report(std::io::stderr().lock(), |output| writeln!(output, "Compactor reset requires every other node, client and upstream writer to remain stopped until completion."));
    if let Err(error) = announced { return close_unstarted(driver, Err(error)).await; }
    run_operation(&handle, driver, &cancellation, async {
        let plan = match retained_base.as_deref() {
            Some(export) => handle.node().prepare_compactor_reset_from_retained_base(&database, export, &cancellation).await?,
            None => handle.node().prepare_compactor_reset(&database, &cancellation).await?,
        };
        let kind = if retained_base.is_some() { "replayed primary" } else { "verified L0" };
        write_report(std::io::stdout().lock(), |output| {
            for (table, tries) in plan.replacement_tries() { writeln!(output, "{}: {} {kind} tries", table.schema_and_table(), tries.len())?; }
            for key in plan.files_to_delete() { writeln!(output, "{} {key}", if dry_run { "WOULD RETIRE" } else { "RETIRE AFTER PUBLICATION" })?; }
            Ok(())
        })?;
        if dry_run { return Ok(()); }
        let block = tokio::time::timeout(Duration::from_secs(300), plan.apply(&cancellation)).await
            .map_err(|_| Anomaly::fault("rtdb.compactor-reset/timeout", "Compactor reset did not complete within five minutes"))??;
        write_report(std::io::stderr().lock(), |output| writeln!(output, "Compactor reset completed at block {}. Compacted files submitted for retirement.", block.0))
    }).await
}

async fn run_operation<T>(handle: &ServerHandle, driver: ServerDriver, cancellation: &tokio_util::sync::CancellationToken, operation: impl std::future::Future<Output = Result<T>>) -> Result<T> {
    let mut operation = Box::pin(operation);
    let mut owner = Box::pin(driver.run());
    let mut owner_finished = false;
    let outcome = tokio::select! {
        result = &mut operation => result,
        result = &mut owner => {
            owner_finished = true;
            cancellation.cancel();
            let _ = tokio::time::timeout(Duration::from_secs(10), &mut operation).await;
            match result {
                Err(error) => Err(error),
                Ok(()) => Err(Anomaly::fault("rtdb.cli/node-stopped", "Node stopped before the maintenance operation completed")),
            }
        }
        signal = shutdown_signal() => {
            cancellation.cancel();
            let stopped = tokio::time::timeout(Duration::from_secs(10), &mut operation).await;
            match (signal, stopped) {
                (Err(error), _) => Err(error),
                (Ok(()), Ok(result)) => result,
                (Ok(()), Err(_)) => Err(Anomaly::fault("rtdb.cli/shutdown-timeout", "Maintenance operation did not stop within ten seconds")),
            }
        }
    };
    drop(operation);
    handle.shutdown();
    let joined = if owner_finished { Ok(()) } else {
        join_with_deadline(owner, tokio::time::sleep(Duration::from_secs(10)), Anomaly::fault("rtdb.server/shutdown-timeout", "Maintenance node did not stop within ten seconds")).await
    };
    after_close(outcome, joined)
}

async fn run_server(handle: &ServerHandle, driver: ServerDriver, signal: impl std::future::Future<Output = Result<()>>) -> Result<()> {
    let mut owner = Box::pin(driver.run());
    let signal = tokio::select! {
        result = &mut owner => return result,
        result = signal => result,
    };
    handle.shutdown();
    let joined = join_with_deadline(owner, tokio::time::sleep(Duration::from_secs(10)), Anomaly::fault("rtdb.server/shutdown-timeout", "Server did not stop within ten seconds")).await;
    after_close(signal, joined)
}

async fn close_unstarted<T>(driver: ServerDriver, outcome: Result<T>) -> Result<T> {
    after_close(outcome, driver.close_without_run().await)
}

async fn join_with_deadline<T>(owner: impl std::future::Future<Output = Result<T>>, deadline: impl std::future::Future<Output = ()>, expired: Anomaly) -> Result<T> {
    let mut owner = Box::pin(owner);
    tokio::select! {
        biased;
        result = &mut owner => return result,
        () = deadline => {},
    }
    let _ = writeln!(std::io::stderr().lock(), "{expired}; waiting for shutdown to finish");
    match owner.await {
        Ok(_) => Err(expired),
        Err(error) => Err(expired.with_context("resource-close-error", error.to_string().into())),
    }
}

fn after_close<T>(outcome: Result<T>, closed: Result<()>) -> Result<T> {
    match (outcome, closed) {
        (Ok(value), Ok(())) => Ok(value),
        (Ok(_), Err(error)) | (Err(error), Ok(())) => Err(error),
        (Err(error), Err(close)) => Err(error.with_context("resource-close-error", close.to_string().into())),
    }
}

fn write_report(mut output: impl Write, report: impl FnOnce(&mut dyn Write) -> std::io::Result<()>) -> Result<()> {
    report(&mut output).and_then(|_| output.flush()).map_err(signal_error)
}

#[cfg(unix)]
async fn shutdown_signal() -> Result<()> {
    let mut interrupt = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::interrupt()).map_err(signal_error)?;
    let mut terminate = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate()).map_err(signal_error)?;
    tokio::select! { _ = interrupt.recv() => Ok(()), _ = terminate.recv() => Ok(()) }
}
#[cfg(not(unix))]
async fn shutdown_signal() -> Result<()> { tokio::signal::ctrl_c().await.map_err(signal_error) }
fn signal_error(error: impl std::fmt::Display) -> Anomaly { Anomaly::fault("rtdb.server/process", error.to_string()) }

#[cfg(test)]
mod tests {
    use super::*;
    use std::{future::{Future, poll_fn, ready}, sync::{Arc, atomic::{AtomicBool, Ordering}}, task::Poll};
    use rtdb_server::config::NodeConfiguration;

    struct DropFlag(Arc<AtomicBool>);
    impl Drop for DropFlag { fn drop(&mut self) { self.0.store(true, Ordering::SeqCst); } }
    struct BrokenOutput;
    impl Write for BrokenOutput {
        fn write(&mut self, _: &[u8]) -> std::io::Result<usize> { Err(std::io::ErrorKind::BrokenPipe.into()) }
        fn flush(&mut self) -> std::io::Result<()> { Ok(()) }
    }

    #[tokio::test]
    async fn shutdown_deadline_reports_failure_only_after_the_owner_finishes() {
        let dropped = Arc::new(AtomicBool::new(false));
        let completed = Arc::new(AtomicBool::new(false));
        let (release, wait) = tokio::sync::oneshot::channel();
        let owner = {
            let (dropped, completed) = (Arc::clone(&dropped), Arc::clone(&completed));
            async move {
                let _guard = DropFlag(dropped);
                wait.await.unwrap(); completed.store(true, Ordering::SeqCst); Ok(())
            }
        };
        let mut joined = Box::pin(join_with_deadline(owner, ready(()), Anomaly::fault("test/deadline", "expired")));
        poll_fn(|context| { assert!(joined.as_mut().poll(context).is_pending()); Poll::Ready(()) }).await;
        assert!(!dropped.load(Ordering::SeqCst)); assert!(!completed.load(Ordering::SeqCst));
        release.send(()).unwrap();
        assert_eq!(joined.await.unwrap_err().code, "test/deadline");
        assert!(completed.load(Ordering::SeqCst)); assert!(dropped.load(Ordering::SeqCst));
    }

    #[tokio::test]
    async fn an_already_completed_owner_takes_precedence_over_the_deadline() {
        assert_eq!(join_with_deadline(ready(Ok(42)), ready(()), Anomaly::fault("test/deadline", "expired")).await.unwrap(), 42);
    }

    async fn unopened(directory: &std::path::Path) -> (ServerHandle, ServerDriver) {
        let yaml = format!("server: {{port: 0}}\nflightSql: null\nhealthz: null\ncompactor: {{threads: 0}}\ndiskCache: {{path: {}, maxSizeBytes: 12345}}", directory.display());
        let mut config = NodeConfiguration::from_yaml_with_environment(&yaml, &|_| None, false).unwrap();
        config.indexer.enabled = false;
        ServerDriver::open(config, 128 * 1024 * 1024).await.unwrap()
    }

    async fn assert_closed(handle: ServerHandle, directory: &std::path::Path) {
        assert!(handle.node().cancellation().is_cancelled());
        assert!(tokio::net::TcpStream::connect(handle.addresses().pgwire.unwrap()).await.is_err());
        drop(handle);
        let cache = rtdb_storage::disk_cache::DiskCache::open(directory, 12345, rtdb_columnar::MemoryBudget::new(1 << 20)).unwrap();
        cache.close_and_wait().await;
    }

    #[tokio::test]
    async fn announcement_and_export_capture_errors_close_unstarted_resources() {
        for export_error in [false, true] {
            let directory = tempfile::tempdir().unwrap();
            let (handle, driver) = unopened(directory.path()).await;
            let error = if export_error {
                handle.node().snapshot_export("missing-database", handle.node().now()).await.err().expect("missing database")
            } else { write_report(BrokenOutput, |output| writeln!(output, "maintenance manifest")).unwrap_err() };
            let code = error.code.clone();
            assert_eq!(close_unstarted::<()>(driver, Err(error)).await.unwrap_err().code, code);
            assert_closed(handle, directory.path()).await;
        }
    }

    #[tokio::test]
    async fn signal_failure_still_cancels_and_joins_the_server_owner() {
        let directory = tempfile::tempdir().unwrap();
        let (handle, driver) = unopened(directory.path()).await;
        let error = run_server(&handle, driver, ready(Err(signal_error("injected signal registration failure")))).await.unwrap_err();
        assert_eq!(error.code, "rtdb.server/process");
        assert_closed(handle, directory.path()).await;
    }
}
